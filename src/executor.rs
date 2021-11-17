use async_channel;
use futures::stream::StreamExt;
use hashbrown::HashMap;
use rdma_cm::PostSendOpcode;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::ops::Deref;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

#[allow(unused_imports)]
use tracing::{debug, error, info, span, trace, Level};

use crate::function_name;

use rdma_cm::{CompletionQueue, ProtectionDomain, QueuePair, RdmaMemory};

use crate::control_flow::ControlFlow;
use futures::Stream;
use std::cmp::min;
use std::sync::mpsc::{Receiver, Sender, TryRecvError};
use std::time::Duration;

pub(crate) struct Executor<const WINDOW_SIZE: usize, const SIZE: usize> {
    tasks: Vec<ConnectionTask<WINDOW_SIZE, SIZE>>,
}

#[derive(Copy, Clone)]
pub struct QueueToken {
    work_id: u64,
    task_id: TaskHandle,
}

#[derive(Debug, Copy, Clone)]
pub struct TaskHandle(usize);

pub enum CompletedRequest<T, const SIZE: usize> {
    Pop(RdmaMemory<T, SIZE>),
    Push(RdmaMemory<T, SIZE>),
}

impl<T, const SIZE: usize> CompletedRequest<T, SIZE> {
    pub fn pop_op(self) -> RdmaMemory<T, SIZE> {
        match self {
            CompletedRequest::Pop(memory) => memory,
            CompletedRequest::Push(_) => panic!("Push event instead of pop."),
        }
    }

    pub fn push_op(self) -> RdmaMemory<T, SIZE> {
        match self {
            CompletedRequest::Pop(_memory) => panic!("Push event instead of push."),
            CompletedRequest::Push(memory) => memory,
        }
    }
}

// TODO: Currently we must make sure the protection domain is declared last as we need to deallocate
// all other registered memory before deallocating protection domain. How to fix this?
struct ConnectionTask<const WINDOW_SIZE: usize, const BUFFER_SIZE: usize> {
    memory_pool: Rc<RefCell<VecDeque<RdmaMemory<u8, BUFFER_SIZE>>>>,

    recv_buffers_coroutine: Pin<Box<dyn Future<Output = ()>>>,
    push_coroutine: Pin<Box<dyn Future<Output = ()>>>,

    push_work_sender: async_channel::Sender<WorkRequest<BUFFER_SIZE>>,
    completed_requests: Rc<RefCell<HashMap<u64, CompletedRequest<u8, BUFFER_SIZE>>>>,
    next_pop_work_id: Receiver<u64>,
    work_id_counter: Rc<RefCell<u64>>,
    control_flow: Rc<RefCell<ControlFlow<{ WINDOW_SIZE }>>>,

    completions_coroutine: Pin<Box<dyn Future<Output = ()>>>,
    /// We keep the protection domain around to make sure it doesn't get dropped before
    /// everything else.
    _protection_domain: ProtectionDomain,
}

impl<const WINDOW_SIZE: usize, const BUFFER_SIZE: usize> Executor<WINDOW_SIZE, BUFFER_SIZE> {
    pub fn new() -> Executor<WINDOW_SIZE, BUFFER_SIZE> {
        info!(
            "{} with N={} and Size={}",
            function_name!(),
            WINDOW_SIZE,
            BUFFER_SIZE
        );
        Executor {
            tasks: Vec::with_capacity(100),
        }
    }

    pub fn add_new_connection(
        &mut self,
        control_flow: ControlFlow<WINDOW_SIZE>,
        queue_pair: QueuePair<WINDOW_SIZE, WINDOW_SIZE>,
        protection_domain: ProtectionDomain,
        completion_queue: CompletionQueue<32>,
    ) -> TaskHandle {
        info!("{}", function_name!());

        let (push_work_sender, push_work_receiver) =
            async_channel::unbounded::<WorkRequest<BUFFER_SIZE>>();

        let (ready_pop_work_id, next_pop_work_id) = std::sync::mpsc::channel::<u64>();

        let processed_requests = Rc::new(RefCell::new(HashMap::with_capacity(1000)));
        let completed_requests = Rc::new(RefCell::new(HashMap::with_capacity(1000)));

        // Allocate two times the amount of chunks we specify.
        let memory_pool: VecDeque<RdmaMemory<u8, BUFFER_SIZE>> = protection_domain
            .register_chunk(2 * WINDOW_SIZE)
            .into_iter()
            .collect();

        let work_id_counter = Rc::new(RefCell::new(0));
        let memory_pool = Rc::new(RefCell::new(memory_pool));
        let control_flow = Rc::new(RefCell::new(control_flow));

        let mut ct = ConnectionTask {
            memory_pool: memory_pool.clone(),
            _protection_domain: protection_domain,
            push_coroutine: Box::pin(push_coroutine(
                queue_pair.clone(),
                push_work_receiver,
                control_flow.clone(),
                processed_requests.clone(),
            )),
            recv_buffers_coroutine: Box::pin(receive_coroutine(
                queue_pair,
                control_flow.clone(),
                memory_pool,
                processed_requests.clone(),
                work_id_counter.clone(),
                ready_pop_work_id,
            )),
            completions_coroutine: Box::pin(completions_coroutine(
                control_flow.clone(),
                completion_queue,
                completed_requests.clone(),
                processed_requests.clone(),
            )),
            control_flow,
            push_work_sender,
            completed_requests,
            next_pop_work_id,
            work_id_counter,
        };

        Self::schedule(&mut ct.recv_buffers_coroutine);

        let current_task_id = self.tasks.len();
        self.tasks.push(ct);
        TaskHandle(current_task_id)
    }

    pub fn malloc(&mut self, task: TaskHandle) -> RdmaMemory<u8, BUFFER_SIZE> {
        trace!("{}", function_name!());

        let mut memory_pool = self
            .tasks
            .get_mut(task.0)
            .expect(&format!("Missing task {:?}", task))
            .memory_pool
            .borrow_mut();
        trace!("Malloc: Entries in memory pool: {}", memory_pool.len());
        memory_pool.pop_front().expect("Out of memory!")
    }

    // TODO Make sure this buffer actually belongs to this handle?
    pub fn free(&mut self, task: TaskHandle, mut memory: RdmaMemory<u8, BUFFER_SIZE>) {
        trace!("{}", function_name!());

        memory.reset_access();
        let mut memory_pool = self
            .tasks
            .get_mut(task.0)
            .expect(&format!("Missing task {:?}", task))
            .memory_pool
            .borrow_mut();
        trace!("Free: Entries in memory pool: {}", memory_pool.len());
        memory_pool.push_back(memory)
    }

    pub fn push(
        &mut self,
        task_handle: TaskHandle,
        memory: RdmaMemory<u8, BUFFER_SIZE>,
    ) -> QueueToken {
        trace!("{}", function_name!());

        let task: &mut ConnectionTask<WINDOW_SIZE, BUFFER_SIZE> =
            self.tasks.get_mut(task_handle.0).unwrap();

        let work_id: u64 = task.work_id_counter.borrow_mut().clone();
        *task.work_id_counter.borrow_mut() += 1;
        let work = WorkRequest { memory, work_id };

        task.push_work_sender
            .try_send(work)
            .expect("Channel should never be full or dropped.");
        Self::schedule(&mut task.push_coroutine);

        QueueToken {
            work_id,
            task_id: task_handle,
        }
    }

    pub fn pop(&mut self, task_handle: TaskHandle) -> QueueToken {
        trace!("{}", function_name!());

        let task: &mut ConnectionTask<WINDOW_SIZE, BUFFER_SIZE> =
            self.tasks.get_mut(task_handle.0).unwrap();

        let work_id = match task.next_pop_work_id.try_recv() {
            Ok(work_id) => work_id,
            Err(TryRecvError::Empty) => {
                info!("Allocating more receive buffers.");
                // Allocate more recv buffers.
                assert_eq!(0, task.control_flow.borrow().remaining_receive_windows());
                Self::schedule(&mut task.recv_buffers_coroutine);
                info!("Done calling recv_buffers coroutine.");
                task.next_pop_work_id
                    .try_recv()
                    .expect("Could not allocate more recv buffers?")
            }
            Err(TryRecvError::Disconnected) => panic!("next_pop_work_id disconnected"),
        };

        QueueToken {
            work_id,
            task_id: task_handle,
        }
    }

    fn schedule(task: &mut Pin<Box<dyn Future<Output = ()>>>) {
        trace!("{}", function_name!());

        let waker = crate::waker::emtpy_waker();
        if let Poll::Ready(_) = task.as_mut().poll(&mut Context::from_waker(&waker)) {
            panic!("Our coroutines should never finish!")
        }
    }

    pub fn background_tasks(&mut self, qt: QueueToken) {
        trace!("{}", function_name!());

        let task: &mut ConnectionTask<WINDOW_SIZE, BUFFER_SIZE> =
            self.tasks.get_mut(qt.task_id.0).unwrap();
        Self::schedule(&mut task.completions_coroutine);
        Self::schedule(&mut task.push_coroutine);
        Self::schedule(&mut task.recv_buffers_coroutine);
    }

    pub fn poll_all_tasks(&mut self) {
        trace!("{}", function_name!());

        for t in self.tasks.iter_mut() {
            Self::schedule(&mut t.completions_coroutine);
            Self::schedule(&mut t.push_coroutine);
            Self::schedule(&mut t.recv_buffers_coroutine);
        }
    }

    /// Checks if work corresponding to `qt` is finished returning the data. Otherwise returns
    /// None.
    pub fn wait(&mut self, qt: QueueToken) -> Option<CompletedRequest<u8, BUFFER_SIZE>> {
        trace!("{}", function_name!());

        let task: &mut ConnectionTask<WINDOW_SIZE, BUFFER_SIZE> =
            self.tasks.get_mut(qt.task_id.0).unwrap();
        task.completed_requests.borrow_mut().remove(&qt.work_id)
    }
}

struct WorkRequest<const SIZE: usize> {
    memory: RdmaMemory<u8, SIZE>,
    work_id: u64,
}

struct SendWindows<const WINDOW_SIZE: usize> {
    control_flow: Rc<RefCell<ControlFlow<WINDOW_SIZE>>>,
}

/// Pending until more send windows are allocated by other side.
impl<const WINDOW_SIZE: usize> Stream for SendWindows<WINDOW_SIZE> {
    type Item = u64;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut cf = self.control_flow.borrow_mut();
        match cf.remaining_send_windows() {
            // Our local variable shows we have exhausted the send windows. Check if other side
            // has allocated more...
            0 => {
                trace!("Out of send windows. Checking if other side has allocated more...");

                // Yes. Other side _has_ allocated more receive windows. Update and we are ready.
                let recv_windows = cf.other_side_recv_windows();
                if recv_windows != 0 {
                    info!("Other side has allocated more recv windows!");
                    cf.remaining_send_window = recv_windows;
                    cf.ack_peer_recv_windows();
                    return Poll::Ready(Some(recv_windows));
                }
                Poll::Pending
            }
            n => Poll::Ready(Some(n)),
        }
    }
}

// Note: Make sure you don't hold RefCells across yield points.
async fn push_coroutine<const WINDOW_SIZE: usize, const SIZE: usize>(
    mut queue_pairs: QueuePair<WINDOW_SIZE, WINDOW_SIZE>,
    push_work: async_channel::Receiver<WorkRequest<SIZE>>,
    control_flow: Rc<RefCell<ControlFlow<WINDOW_SIZE>>>,
    processed_requests: Rc<RefCell<HashMap<u64, RdmaMemory<u8, SIZE>>>>,
) {
    let s = span!(Level::INFO, "push_coroutine");
    s.in_scope(|| debug!("started!"));
    let mut send_windows = SendWindows {
        control_flow: control_flow.clone(),
    };

    let mut work_requests: VecDeque<WorkRequest<SIZE>> = VecDeque::with_capacity(WINDOW_SIZE);
    let mut requests: VecDeque<(u64, RdmaMemory<u8, SIZE>)> = VecDeque::with_capacity(WINDOW_SIZE);

    loop {
        let available_windows = send_windows
            .next()
            .await
            .expect("Our streams should never end.");

        s.in_scope(|| debug!("{} send windows currently available.", available_windows));

        if work_requests.is_empty() {
            // Yield until something comes along.
            work_requests.push_back(push_work.recv().await.unwrap())
        }
        // Add all other entries now that we know we have at least one.
        for _ in 0..push_work.len() {
            work_requests.push_back(push_work.try_recv().expect("TODO"));
        }

        // Send as many requests as possible based on the available windows.
        let requests_number = min(work_requests.len(), available_windows as usize);
        s.in_scope(|| debug!("Sending {} requests.", requests_number));

        for wr in work_requests.drain(..requests_number) {
            requests.push_back((wr.work_id, wr.memory));
        }

        queue_pairs.post_send(requests.iter(), PostSendOpcode::Send);

        let mut processed_push_requests = processed_requests.borrow_mut();
        for (work_id, memory) in requests.drain(..requests_number) {
            assert!(
                processed_push_requests.insert(work_id, memory).is_none(),
                "duplicate entry"
            );
        }

        control_flow
            .borrow_mut()
            .subtract_remaining_send_windows(requests_number as u64);
    }
}

struct RemainingReceiveWindows<const WINDOW_SIZE: usize> {
    control_flow: Rc<RefCell<ControlFlow<WINDOW_SIZE>>>,
}

/// Pending until more send windows are allocated by other side.
impl<const WINDOW_SIZE: usize> Stream for RemainingReceiveWindows<WINDOW_SIZE> {
    type Item = u64;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let cf = self.control_flow.deref().borrow();
        match cf.remaining_receive_windows() {
            0 => Poll::Ready(Some(WINDOW_SIZE as u64)),
            _ => Poll::Pending,
        }
    }
}

async fn receive_coroutine<const WINDOW_SIZE: usize, const SIZE: usize>(
    mut queue_pair: QueuePair<WINDOW_SIZE, WINDOW_SIZE>,
    control_flow: Rc<RefCell<ControlFlow<WINDOW_SIZE>>>,
    // Reference to our Executor's memory poll. We take entries for here for our post_receive RDMA
    // operation.
    memory_pool: Rc<RefCell<VecDeque<RdmaMemory<u8, SIZE>>>>,
    processed_requests: Rc<RefCell<HashMap<u64, RdmaMemory<u8, SIZE>>>>,
    // Actual counter used to keep track of what work_id we are on. This value is shared with
    // with the push operation that increments it by 1. We increment it by `how_many` based on
    // the new number of recv windows to allocate. But pop needs to know what numbers we reserved
    // for recv buffer pops. Thus, the need for both work_id_counter and ready_pop_work_id.
    work_id_counter: Rc<RefCell<u64>>,
    // Our `pop` operation knows what work ID to assign to the next based on the integers we
    // send down this channel.
    ready_pop_work_id: Sender<u64>,
) {
    let mut receive_buffers: Vec<(u64, RdmaMemory<u8, SIZE>)> = Vec::with_capacity(WINDOW_SIZE);

    let s = span!(Level::INFO, "post_receive_coroutine");
    s.in_scope(|| debug!("started!"));
    let mut recv_windows = RemainingReceiveWindows {
        control_flow: control_flow.clone(),
    };

    loop {
        s.in_scope(|| info!("Awaiting..."));
        let how_many = recv_windows
            .next()
            .await
            .expect("Our streams should never end.");

        s.in_scope(|| info!("...Allocating {} new receive buffers!", how_many));

        let work_id: u64 = work_id_counter.deref().borrow().clone();

        for i in work_id..work_id + how_many {
            let memory = memory_pool
                .borrow_mut()
                .pop_front()
                .expect("Memory pool is empty.");
            receive_buffers.push((i, memory));
        }

        queue_pair.post_receive(receive_buffers.iter());

        s.in_scope(|| {
            debug!(
                "Work ids {} through {} are ready.",
                work_id,
                work_id + how_many
            )
        });

        let mut processed_requests = processed_requests.borrow_mut();
        // info!("Receive buffers added: {}", receive_buffers.len());
        for (work_id, memory) in receive_buffers.drain(..how_many as usize) {
            ready_pop_work_id.send(work_id).unwrap();
            assert!(
                processed_requests.insert(work_id, memory).is_none(),
                "duplicate entry"
            );
        }
        *work_id_counter.borrow_mut() += how_many;
        control_flow.borrow_mut().add_recv_windows(how_many);
    }
}

async fn completions_coroutine<
    const WINDOW_SIZE: usize,
    const CQ_MAX_ELEMENTS: usize,
    const SIZE: usize,
>(
    control_flow: Rc<RefCell<ControlFlow<WINDOW_SIZE>>>,
    cq: CompletionQueue<CQ_MAX_ELEMENTS>,
    completed_requests: Rc<RefCell<HashMap<u64, CompletedRequest<u8, SIZE>>>>,
    processed_requests: Rc<RefCell<HashMap<u64, RdmaMemory<u8, SIZE>>>>,
) -> () {
    let s = span!(Level::INFO, "completions_coroutine");
    s.in_scope(|| info!("started!"));

    let mut event_stream = AsyncCompletionQueue::<CQ_MAX_ELEMENTS> { cq };
    // It might looks like this line doesn't do anything but it does. We need `control_flow`
    // to get dropped before completion queue. As the queue pair inside control flow must
    // be deallocated before the completion queue. This ensures control_flow is dropped
    // before cq... Sorry.
    let control_flow = control_flow;
    loop {
        let completed = event_stream
            .next()
            .await
            .expect("Our stream should never end.");

        s.in_scope(|| debug!("{} events completed!.", completed.len()));

        let mut recv_requests_completed = 0;
        let mut completed_requests = completed_requests.borrow_mut();
        let mut processed_requests = processed_requests.borrow_mut();

        for c in completed {
            s.in_scope(|| trace!("Work completion status for {}: {}", c.wr_id, c.status));
            if c.status != rdma_cm::ffi::ibv_wc_status_IBV_WC_SUCCESS {
                panic!("Completion queue event with error status {}.", c.status);
            }

            if c.opcode == rdma_cm::ffi::ibv_wc_opcode_IBV_WC_RECV {
                let mut memory = processed_requests.remove(&c.wr_id).
                    // This should be impossible.
                    expect("Processed entry for completed wr missing.");

                recv_requests_completed += 1;
                let bytes_transferred = c.byte_len as usize;
                memory.initialize_length(bytes_transferred);
                assert!(
                    completed_requests
                        .insert(c.wr_id, CompletedRequest::Pop(memory))
                        .is_none(),
                    "duplicate entry"
                );
            } else if c.opcode == rdma_cm::ffi::ibv_wc_opcode_IBV_WC_SEND {
                let memory = processed_requests.remove(&c.wr_id).
                    // This should be impossible.
                    expect("Processed entry for completed wr missing.");

                assert!(
                    completed_requests
                        .insert(c.wr_id, CompletedRequest::Push(memory))
                        .is_none(),
                    "duplicate entry"
                );
            } else if c.opcode == rdma_cm::ffi::ibv_wc_opcode_IBV_WC_RDMA_WRITE {
                debug!("RDMA Write succeeded.");
            } else if c.opcode == rdma_cm::ffi::ibv_wc_opcode_IBV_WC_RDMA_READ {
                debug!("RDMA Read succeeded.");
            } else {
                panic!("Unknown ibv_wc opcode: {:?}", c.opcode);
            }
        }

        control_flow
            .borrow_mut()
            .subtract_recv_windows(recv_requests_completed);
    }
}

struct AsyncCompletionQueue<const CQ_MAX_ELEMENTS: usize> {
    cq: rdma_cm::CompletionQueue<CQ_MAX_ELEMENTS>,
}

impl<const CQ_MAX_ELEMENTS: usize> Stream for AsyncCompletionQueue<CQ_MAX_ELEMENTS> {
    type Item = arrayvec::IntoIter<rdma_cm::ffi::ibv_wc, CQ_MAX_ELEMENTS>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.cq.poll() {
            None => Poll::Pending,
            Some(entries) => Poll::Ready(Some(entries)),
        }
    }
}
