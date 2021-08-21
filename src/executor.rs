use async_channel;
use futures::stream::StreamExt;
use rdma_cm::PostSendOpcode;
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::ops::Deref;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

#[allow(unused_imports)]
use tracing::{debug, info, span, trace, Level};

use crate::function_name;

use rdma_cm::{CompletionQueue, ProtectionDomain, QueuePair, RegisteredMemory};

use crate::control_flow::ControlFlow;
use futures::Stream;
use std::cmp::min;
use std::collections::hash_map::Entry;
use std::sync::mpsc::{Receiver, Sender, TryRecvError};

pub(crate) struct Executor<const N: usize, const SIZE: usize> {
    tasks: Vec<ConnectionTask<SIZE>>,
}

#[derive(Copy, Clone)]
pub struct QueueToken {
    work_id: u64,
    task_id: TaskHandle,
}

#[derive(Debug, Copy, Clone)]
pub struct TaskHandle(usize);

enum CompletedRequest<const SIZE: usize> {
    /// Number of bytes received. Used to initialize registered memory.
    Pop(RegisteredMemory<u8, SIZE>, usize),
    Push(RegisteredMemory<u8, SIZE>),
}

// TODO: Currently we must make sure the protection domain is declared last as we need to deallocate
// all other registered memory before deallocating protection domain. How to fix this?
struct ConnectionTask<const SIZE: usize> {
    recv_buffers_coroutine: Pin<Box<dyn Future<Output = ()>>>,

    memory_pool: Rc<RefCell<VecDeque<RegisteredMemory<u8, SIZE>>>>,
    push_coroutine: Pin<Box<dyn Future<Output = ()>>>,
    completions_coroutine: Pin<Box<dyn Future<Output = ()>>>,

    push_work_sender: async_channel::Sender<WorkRequest<SIZE>>,

    processed_requests: Rc<RefCell<HashMap<u64, RegisteredMemory<u8, SIZE>>>>,
    completed_requests: Rc<RefCell<HashMap<u64, CompletedRequest<SIZE>>>>,

    next_pop_work_id: Receiver<u64>,

    control_flow: Rc<RefCell<ControlFlow>>,
    work_id_counter: Rc<RefCell<u64>>,
    protection_domain: ProtectionDomain,
}

impl<const N: usize, const SIZE: usize> Executor<N, SIZE> {
    pub fn new() -> Executor<N, SIZE> {
        info!("{} with N={} and Size={}", function_name!(), N, SIZE);
        Executor {
            tasks: Vec::with_capacity(100),
        }
    }

    pub fn add_new_connection(
        &mut self,
        control_flow: ControlFlow,
        queue_pair: QueuePair,
        protection_domain: ProtectionDomain,
        completion_queue: CompletionQueue<25>,
    ) -> TaskHandle {
        info!("{}", function_name!());

        let (push_work_sender, push_work_receiver) =
            async_channel::unbounded::<WorkRequest<SIZE>>();

        let (ready_pop_work_id, next_pop_work_id) = std::sync::mpsc::channel::<u64>();

        let processed_requests = Rc::new(RefCell::new(HashMap::with_capacity(1000)));
        let completed_requests = Rc::new(RefCell::new(HashMap::with_capacity(1000)));

        let control_flow = Rc::new(RefCell::new(control_flow));

        // Allocate two times as many vectors as our
        let mut memory_pool: VecDeque<RegisteredMemory<u8, SIZE>> = VecDeque::with_capacity(2 * N);
        for _ in 0..2 * N {
            let memory = protection_domain.allocate_memory::<u8, SIZE>();
            memory_pool.push_back(memory);
        }

        let work_id_counter = Rc::new(RefCell::new(0));
        let memory_pool = Rc::new(RefCell::new(memory_pool));

        let mut ct = ConnectionTask {
            memory_pool: memory_pool.clone(),
            protection_domain,
            push_coroutine: Box::pin(push_coroutine(
                queue_pair.clone(),
                push_work_receiver,
                control_flow.clone(),
                processed_requests.clone(),
            )),
            recv_buffers_coroutine: Box::pin(post_receive_coroutine(
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
            push_work_sender,
            processed_requests,
            completed_requests,
            next_pop_work_id,
            control_flow,
            work_id_counter,
        };

        info!("Starting coroutines.");
        Self::schedule(&mut ct.recv_buffers_coroutine);
        // Self::schedule(&mut ct.push_coroutine);
        // Self::schedule(&mut ct.completions_coroutine);

        let current_task_id = self.tasks.len();
        self.tasks.push(ct);
        TaskHandle(current_task_id)
    }

    pub fn malloc(&mut self, task: TaskHandle) -> RegisteredMemory<u8, SIZE> {
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
    pub fn free(&mut self, task: TaskHandle, mut memory: RegisteredMemory<u8, SIZE>) {
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
        memory: RegisteredMemory<u8, SIZE>,
    ) -> QueueToken {
        trace!("{}", function_name!());

        let task: &mut ConnectionTask<SIZE> = self.tasks.get_mut(task_handle.0).unwrap();

        let work_id: u64 = task.work_id_counter.borrow_mut().clone();
        *task.work_id_counter.borrow_mut() += 1;
        let work = WorkRequest { memory, work_id };

        task.push_work_sender
            .try_send(work)
            .expect("Channel should never be full or dropped.");
        // TODO: Is push coroutine called too often?
        Self::schedule(&mut task.push_coroutine);

        QueueToken {
            work_id,
            task_id: task_handle,
        }
    }

    pub fn pop(&mut self, task_handle: TaskHandle) -> QueueToken {
        trace!("{}", function_name!());

        let task: &mut ConnectionTask<SIZE> = self.tasks.get_mut(task_handle.0).unwrap();

        let work_id = match task.next_pop_work_id.try_recv() {
            Ok(work_id) => work_id,
            Err(TryRecvError::Empty) => {
                debug!("Allocating more receive buffers.");
                // Allocate more recv buffers.
                Self::schedule(&mut task.recv_buffers_coroutine);
                task.next_pop_work_id
                    .try_recv()
                    .expect("Could not allocate more recv buffers")
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

    pub fn service_completion_queue(&mut self, qt: QueueToken) {
        trace!("{}", function_name!());

        let task: &mut ConnectionTask<SIZE> = self.tasks.get_mut(qt.task_id.0).unwrap();
        Self::schedule(&mut task.completions_coroutine);
        Self::schedule(&mut task.recv_buffers_coroutine);
    }

    pub fn wait(&mut self, qt: QueueToken) -> Option<RegisteredMemory<u8, SIZE>> {
        trace!("{}", function_name!());

        let task: &mut ConnectionTask<SIZE> = self.tasks.get_mut(qt.task_id.0).unwrap();

        match task.completed_requests.borrow_mut().entry(qt.work_id) {
            Entry::Occupied(entry) => {
                match entry.remove() {
                    CompletedRequest::Pop(mut memory, bytes_transferred) => {
                        // Access `bytes_transferred` number of bytes to
                        memory.initialize_length(bytes_transferred);
                        Some(memory)
                    }
                    CompletedRequest::Push(memory) => Some(memory),
                }
            }
            // Work request not yet ready.
            Entry::Vacant(_) => None,
        }
    }
}

struct WorkRequest<const SIZE: usize> {
    memory: RegisteredMemory<u8, SIZE>,
    work_id: u64,
}

struct SendWindows {
    control_flow: Rc<RefCell<ControlFlow>>,
}

/// Pending until more send windows are allocated by other side.
impl Stream for SendWindows {
    type Item = u64;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.control_flow.borrow_mut().remaining_send_windows() {
            // Our local variable shows we have exhausted the send windows. Check if other side
            // has allocated more
            0 => {
                info!("Out of send windows. Checking if other side has allocated more...");
                Poll::Pending
            }
            n => Poll::Ready(Some(n)),
        }
    }
}

// Note: Make sure you don't hold RefCells across yield points.
async fn push_coroutine<const SIZE: usize>(
    mut queue_pairs: QueuePair,
    push_work: async_channel::Receiver<WorkRequest<SIZE>>,
    control_flow: Rc<RefCell<ControlFlow>>,
    processed_requests: Rc<RefCell<HashMap<u64, RegisteredMemory<u8, SIZE>>>>,
) {
    let s = span!(Level::INFO, "push_coroutine");
    s.in_scope(|| debug!("started!"));
    let mut send_windows = SendWindows {
        control_flow: control_flow.clone(),
    };

    let mut work_requests: VecDeque<WorkRequest<SIZE>> = VecDeque::with_capacity(1000);

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

        let range = min(work_requests.len(), available_windows as usize);
        s.in_scope(|| debug!("Sending {} requests.", range));

        let requests_to_send: VecDeque<(u64, RegisteredMemory<u8, SIZE>)> = work_requests
            .drain(..range)
            .map(|wr| (wr.work_id, wr.memory))
            .collect();

        queue_pairs.post_send(requests_to_send.iter(), PostSendOpcode::Send);

        let mut processed_push_requests = processed_requests.borrow_mut();
        for (work_id, memory) in requests_to_send {
            assert!(
                processed_push_requests.insert(work_id, memory).is_none(),
                "duplicate entry"
            );
        }
        // TODO Subtract send window!
    }
}

struct RemainingReceiveWindows {
    control_flow: Rc<RefCell<ControlFlow>>,
}

/// Pending until more send windows are allocated by other side.
impl Stream for RemainingReceiveWindows {
    type Item = u64;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let cf = self.control_flow.deref().borrow();
        match cf.remaining_receive_windows() {
            0 => Poll::Ready(Some(cf.batch_size)),
            _ => Poll::Pending,
        }
    }
}

async fn post_receive_coroutine<const SIZE: usize>(
    mut queue_pair: QueuePair,
    control_flow: Rc<RefCell<ControlFlow>>,
    memory_pool: Rc<RefCell<VecDeque<RegisteredMemory<u8, SIZE>>>>,
    processed_requests: Rc<RefCell<HashMap<u64, RegisteredMemory<u8, SIZE>>>>,
    work_id_counter: Rc<RefCell<u64>>,
    ready_pop_work_id: Sender<u64>,
) {
    let s = span!(Level::INFO, "post_receive_coroutine");
    s.in_scope(|| debug!("started!"));
    let mut recv_windows = RemainingReceiveWindows {
        control_flow: control_flow.clone(),
    };

    loop {
        let how_many = recv_windows
            .next()
            .await
            .expect("Our streams should never end.");

        s.in_scope(|| info!("Allocating {} new receive buffers!", how_many));

        let work_id: u64 = work_id_counter.deref().borrow().clone();
        let mut receive_buffers: Vec<(u64, RegisteredMemory<u8, SIZE>)> =
            Vec::with_capacity(how_many as usize);

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
        for (work_id, memory) in receive_buffers {
            ready_pop_work_id.send(work_id).unwrap();
            processed_requests.insert(work_id, memory);
        }

        control_flow.borrow_mut().add_recv_windows(how_many);
        *work_id_counter.borrow_mut() += how_many;
    }
}

async fn completions_coroutine<const CQ_MAX_ELEMENTS: usize, const SIZE: usize>(
    control_flow: Rc<RefCell<ControlFlow>>,
    cq: CompletionQueue<CQ_MAX_ELEMENTS>,
    completed_requests: Rc<RefCell<HashMap<u64, CompletedRequest<SIZE>>>>,
    processed_requests: Rc<RefCell<HashMap<u64, RegisteredMemory<u8, SIZE>>>>,
) -> () {
    let s = span!(Level::INFO, "completions_coroutine");
    s.in_scope(|| info!("started!"));
    let mut event_stream = AsyncCompletionQueue::<CQ_MAX_ELEMENTS> { cq };

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
            let memory = processed_requests.remove(&c.wr_id).
                // This should be impossible.
                expect("Processed entry for completed wr missing.");

            // TODO: this if/else assumes if its not a RECV it is a SEND. But there are
            // others.
            if c.opcode == rdma_cm::ffi::ibv_wc_opcode_IBV_WC_RECV {
                recv_requests_completed += 1;
                // TODO assert request wasn't here before.
                let bytes_transferred = c.byte_len as usize;
                completed_requests
                    .insert(c.wr_id, CompletedRequest::Pop(memory, bytes_transferred));
            } else {
                // TODO assert request wasn't here before.
                completed_requests.insert(c.wr_id, CompletedRequest::Push(memory));
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