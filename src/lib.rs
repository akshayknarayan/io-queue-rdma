use std::ptr::null_mut;

use nix::sys::socket::SockAddr;
use rdma_cm;
use rdma_cm::{
    CommunicationManager, PeerConnectionData, RdmaCmEvent, RdmaMemory, VolatileRdmaMemory,
};

use crate::executor::{Executor, QueueTokenOp, TaskHandle, TIME};
use control_flow::ControlFlow;
pub use executor::{CompletedRequest, QueueToken};

mod control_flow;
mod executor;
mod utils;
mod waker;
#[allow(unused_imports)]
use tracing::{debug, info, trace, Level};

pub struct QueueDescriptor<const BLOCKING: bool> {
    cm: rdma_cm::CommunicationManager<BLOCKING>,
    // TODO a better API could avoid having these as options
    scheduler_handle: Option<TaskHandle>,
}

pub struct IoQueue<
    const RECV_WRS: usize,
    const SEND_WRS: usize,
    const CQ_ELEMENTS: usize,
    const WINDOW_SIZE: usize,
    const BUFFER_SIZE: usize,
    const BLOCKING: bool,
> {
    executor: executor::Executor<RECV_WRS, SEND_WRS, CQ_ELEMENTS, WINDOW_SIZE, BUFFER_SIZE>,
}

impl<
        const RECV_WRS: usize,
        const SEND_WRS: usize,
        const CQ_ELEMENTS: usize,
        const WINDOW_SIZE: usize,
        const BUFFER_SIZE: usize,
    > IoQueue<RECV_WRS, SEND_WRS, CQ_ELEMENTS, WINDOW_SIZE, BUFFER_SIZE, true>
{
    pub fn new() -> IoQueue<RECV_WRS, SEND_WRS, CQ_ELEMENTS, WINDOW_SIZE, BUFFER_SIZE, true> {
        info!("{}", function_name!());
        IoQueue {
            executor: Executor::new(),
        }
    }

    pub fn set_async(
        self,
    ) -> IoQueue<RECV_WRS, SEND_WRS, CQ_ELEMENTS, WINDOW_SIZE, BUFFER_SIZE, false> {
        IoQueue {
            executor: self.executor,
        }
    }
}

impl<
        const RECV_WRS: usize,
        const SEND_WRS: usize,
        const CQ_ELEMENTS: usize,
        const WINDOW_SIZE: usize,
        const BUFFER_SIZE: usize,
        const BLOCKING: bool,
    > IoQueue<RECV_WRS, SEND_WRS, CQ_ELEMENTS, WINDOW_SIZE, BUFFER_SIZE, BLOCKING>
{
    pub fn bind(
        &mut self,
        qd: &mut QueueDescriptor<BLOCKING>,
        socket_address: &SockAddr,
    ) -> Result<(), ()> {
        info!("{}", function_name!());
        qd.cm.bind(socket_address).expect("TODO");
        Ok(())
    }

    pub fn listen(&mut self, qd: &mut QueueDescriptor<BLOCKING>) {
        info!("{}", function_name!());

        qd.cm.listen().expect("TODO");
    }

    // should these executor functions be async?

    /// Fetch a buffer from our pre-allocated memory pool.
    /// TODO: This function should only be called once the protection domain has been allocated.
    pub fn malloc(&mut self, qd: &mut QueueDescriptor<BLOCKING>) -> RdmaMemory<u8, BUFFER_SIZE> {
        trace!("{}", function_name!());

        // TODO Do proper error handling. This expect means the connection was never properly
        // established via accept or connect. So we never added it to the executor.
        self.executor
            .malloc(qd.scheduler_handle.expect("Missing executor handle."))
    }

    pub fn free<const B: bool>(
        &mut self,
        qd: &mut QueueDescriptor<B>,
        memory: RdmaMemory<u8, BUFFER_SIZE>,
    ) {
        trace!("{}", function_name!());
        // TODO Do proper error handling. This expect means the connection was never properly
        // established via accept or connect. So we never added it to the executor.
        self.executor.free(
            qd.scheduler_handle.expect("Missing executor handle."),
            memory,
        );
    }

    /// We will need to use the lower level ibverbs interface to register UserArrays with
    /// RDMA on behalf of the user.
    /// TODO: If user drops QueueToken we will be pointing to dangling memory... We should reference
    /// count he memory ourselves...
    pub fn push<const B: bool>(
        &mut self,
        qd: &mut QueueDescriptor<B>,
        mem: RdmaMemory<u8, BUFFER_SIZE>,
    ) -> QueueToken {
        trace!("{}", function_name!());

        let error = "Passed queue descriptor has no scheduler associated wit it!\
                     You likely passed the connection listener descriptor instead\
                     of the connection descriptor.";
        let handle = qd.scheduler_handle.expect(error);
        self.executor.push(handle, mem)
    }

    /// TODO: Bad things will happen if queue token is dropped as the memory registered with
    /// RDMA will be deallocated.
    pub fn pop<const B: bool>(&mut self, qd: &mut QueueDescriptor<B>) -> QueueToken {
        trace!("{}", function_name!());
        self.executor.pop(qd.scheduler_handle.unwrap())
    }

    pub fn wait(&mut self, qt: QueueToken) -> CompletedRequest<u8, BUFFER_SIZE> {
        trace!("{}", function_name!());
        loop {
            match self.executor.wait(qt) {
                None => match self.executor.poll_completion_coroutine(qt) {
                    None => self.executor.poll_coroutines(qt),
                    Some(cr) => return cr,
                },
                Some(cr) => return cr,
            }
        }
        // loop {
        //     match self.executor.wait(qt) {
        //         None => {
        //             self.executor.poll_coroutines(qt);
        //         }
        //         Some(cr) => return cr,
        //     }
        // }
    }

    pub fn get_and_reset_time(&mut self) -> u32 {
        TIME.with(|time| {
            let current = *time.borrow_mut();
            *time.borrow_mut() = 0;
            current
        })
    }

    pub fn wait_any(&mut self, qts: &[QueueToken]) -> (usize, CompletedRequest<u8, BUFFER_SIZE>) {
        trace!("{}", function_name!());

        let mut pops_checked: bool = false;
        loop {
            for (i, qt) in qts.iter().enumerate() {
                match qt.op {
                    QueueTokenOp::Push { .. } => {
                        if let Some(completed_op) = self.executor.wait(*qt) {
                            return (i, completed_op);
                        }
                    }
                    QueueTokenOp::Pop => {
                        if pops_checked {
                            continue;
                        } else {
                            if let Some(completed_op) = self.executor.wait(*qt) {
                                return (i, completed_op);
                            } else {
                                pops_checked = true;
                            }
                        }
                    }
                }
            }
            self.executor.poll_all_tasks();
            pops_checked = false;
        }
    }
}

impl<
        const RECV_WRS: usize,
        const SEND_WRS: usize,
        const CQ_ELEMENTS: usize,
        const WINDOW_SIZE: usize,
        const BUFFER_SIZE: usize,
    > IoQueue<RECV_WRS, SEND_WRS, CQ_ELEMENTS, WINDOW_SIZE, BUFFER_SIZE, true>
{
    /// Initializes RDMA by fetching the device?
    /// Allocates memory regions?
    pub fn socket(&self) -> QueueDescriptor<true> {
        info!("{}", function_name!());

        let cm = rdma_cm::CommunicationManager::new().expect("TODO");

        QueueDescriptor {
            cm,
            scheduler_handle: None,
        }
    }

    /// There is a lot of setup require for connecting. This function:
    /// 1) resolves address of connection.
    /// 2) resolves route.
    /// 3) Creates protection domain, completion queue, and queue pairs.
    /// 4) Establishes receive window communication.
    pub fn connect(&mut self, qd: &mut QueueDescriptor<true>, node: &str, service: &str) {
        info!("{}", function_name!());

        IoQueue::<RECV_WRS, SEND_WRS, CQ_ELEMENTS, WINDOW_SIZE, BUFFER_SIZE, true>::resolve_address(
            qd, node, service,
        );

        // Resolve route
        qd.cm.resolve_route(1).expect("TODO");
        let event = qd.cm.get_cm_event().expect("TODO");
        assert_eq!(RdmaCmEvent::RouteResolved, event.get_event());
        event.ack();

        // Allocate pd, cq, and qp.
        let mut pd = qd.cm.allocate_protection_domain().expect("TODO");
        let cq = qd.cm.create_cq().expect("TODO");
        let qp = qd.cm.create_qp(&pd, &cq);

        let mut our_recv_window = VolatileRdmaMemory::<u64, 1>::new(&mut pd);
        qd.cm
            .connect_with_data(&our_recv_window.as_connection_data())
            .expect("TODO");

        let event = qd.cm.get_cm_event().expect("TODO");
        assert_eq!(RdmaCmEvent::Established, event.get_event());

        // Server sent us its send_window. Let's save it somewhere.
        let peer: PeerConnectionData<u64, 1> =
            event.get_private_data().expect("Private data missing!");
        dbg!(peer);

        let cf = ControlFlow::new(
            qp.clone(),
            pd.allocate_memory::<u64, 1>(),
            our_recv_window,
            peer,
        );
        qd.scheduler_handle = Some(self.executor.add_new_connection(cf, qp, pd, cq));
    }

    fn resolve_address(qd: &mut QueueDescriptor<true>, node: &str, service: &str) {
        info!("{}", function_name!());

        // Get address info and resolve route!
        let addr_info =
            CommunicationManager::<true>::get_address_info(node, service).expect("TODO");
        let mut current = addr_info;

        // TODO: This will fail if the address is never found.
        let mut address_resolved = false;
        while current != null_mut() {
            match qd.cm.resolve_address((unsafe { *current }).ai_dst_addr) {
                Ok(_) => {
                    address_resolved = true;
                    break;
                }
                Err(_) => {}
            }

            unsafe {
                current = (*current).ai_next;
            }
        }
        if !address_resolved {
            panic!("Unable to resolve address {}:{}", node, service);
        }
        // Ack address resolution.
        let event = qd.cm.get_cm_event().expect("TODO");
        assert_eq!(RdmaCmEvent::AddressResolved, event.get_event());
        event.ack();
    }

    /// NOTE: Accept allocates a protection domain and queue descriptor internally for this id.
    /// And acks establishes connection.
    pub fn accept(&mut self, qd: &mut QueueDescriptor<true>) -> QueueDescriptor<true> {
        info!("{}", function_name!());

        // Block until connection request arrives.
        let event = qd.cm.get_cm_event().expect("TODO");
        assert_eq!(RdmaCmEvent::ConnectionRequest, event.get_event());

        // New connection established! Use this  connection for RDMA communication.
        let connected_id = event.get_connection_request_id();
        let client_private_data: PeerConnectionData<u64, 1> =
            event.get_private_data().expect("Missing private data!");
        event.ack();

        let mut pd = connected_id.allocate_protection_domain().expect("TODO");
        let cq = connected_id.create_cq().expect("TODO");
        let qp = connected_id.create_qp(&pd, &cq);

        // Now send our connection data to client.
        let mut recv_window = VolatileRdmaMemory::new(&mut pd);

        // dbg!(our_private_data);
        connected_id
            .accept_with_private_data(&recv_window.as_connection_data())
            .expect("TODO");
        let event = qd.cm.get_cm_event().expect("TODO");
        assert_eq!(RdmaCmEvent::Established, event.get_event());
        event.ack();

        let control_flow = ControlFlow::new(
            qp.clone(),
            pd.allocate_memory(),
            recv_window,
            client_private_data,
        );
        let scheduler_handle = self.executor.add_new_connection(control_flow, qp, pd, cq);

        QueueDescriptor {
            cm: connected_id,
            scheduler_handle: Some(scheduler_handle),
        }
    }

    pub fn disconnect(&mut self, qd: QueueDescriptor<true>) {
        qd.cm.disconnect().unwrap();
        let event = qd.cm.get_cm_event().unwrap();
        assert_eq!(event.get_event(), RdmaCmEvent::Disconnected);
        event.ack();
    }
}

impl<
        const RECV_WRS: usize,
        const SEND_WRS: usize,
        const CQ_ELEMENTS: usize,
        const WINDOW_SIZE: usize,
        const BUFFER_SIZE: usize,
    > IoQueue<RECV_WRS, SEND_WRS, CQ_ELEMENTS, WINDOW_SIZE, BUFFER_SIZE, false>
{
    pub fn socket(&self) -> QueueDescriptor<false> {
        info!("{}", function_name!());

        let cm = rdma_cm::CommunicationManager::new()
            .expect("TODO")
            .async_cm_events()
            .unwrap();

        QueueDescriptor {
            cm,
            scheduler_handle: None,
        }
    }

    /// There is a lot of setup require for connecting. This function:
    /// 1) resolves address of connection.
    /// 2) resolves route.
    /// 3) Creates protection domain, completion queue, and queue pairs.
    /// 4) Establishes receive window communication.
    pub async fn connect(&mut self, qd: &mut QueueDescriptor<false>, node: &str, service: &str) {
        info!("{}", function_name!());

        IoQueue::<RECV_WRS, SEND_WRS, CQ_ELEMENTS, WINDOW_SIZE, BUFFER_SIZE, false>::resolve_address(
            qd, node, service,
        ).await;

        // Resolve route
        qd.cm.resolve_route(1).expect("TODO");
        let event = qd.cm.get_cm_event().await.expect("TODO");
        assert_eq!(RdmaCmEvent::RouteResolved, event.get_event());
        event.ack();

        // Allocate pd, cq, and qp.
        let mut pd = qd.cm.allocate_protection_domain().expect("TODO");
        let cq = qd.cm.create_cq().expect("TODO");
        let qp = qd.cm.create_qp(&pd, &cq);

        let mut our_recv_window = VolatileRdmaMemory::<u64, 1>::new(&mut pd);
        qd.cm
            .connect_with_data(&our_recv_window.as_connection_data())
            .expect("TODO");

        let event = qd.cm.get_cm_event().await.expect("TODO");
        assert_eq!(RdmaCmEvent::Established, event.get_event());

        // Server sent us its send_window. Let's save it somewhere.
        let peer: PeerConnectionData<u64, 1> =
            event.get_private_data().expect("Private data missing!");
        dbg!(peer);

        let cf = ControlFlow::new(
            qp.clone(),
            pd.allocate_memory::<u64, 1>(),
            our_recv_window,
            peer,
        );
        qd.scheduler_handle = Some(self.executor.add_new_connection(cf, qp, pd, cq));
    }

    async fn resolve_address(qd: &mut QueueDescriptor<false>, node: &str, service: &str) {
        info!("{}", function_name!());

        // Get address info and resolve route!
        let addr_info =
            CommunicationManager::<false>::get_address_info(node, service).expect("TODO");
        let mut current = addr_info;

        // TODO: This will fail if the address is never found.
        let mut address_resolved = false;
        while current != null_mut() {
            match qd.cm.resolve_address((unsafe { *current }).ai_dst_addr) {
                Ok(_) => {
                    address_resolved = true;
                    break;
                }
                Err(_) => {}
            }

            unsafe {
                current = (*current).ai_next;
            }
        }
        if !address_resolved {
            panic!("Unable to resolve address {}:{}", node, service);
        }
        // Ack address resolution.
        let event = qd.cm.get_cm_event().await.expect("TODO");
        assert_eq!(RdmaCmEvent::AddressResolved, event.get_event());
        event.ack();
    }

    pub async fn accept(&mut self, qd: &mut QueueDescriptor<false>) -> QueueDescriptor<true> {
        info!("{}", function_name!());

        // Block until connection request arrives.
        let event = qd.cm.get_cm_event().await.expect("TODO");
        assert_eq!(RdmaCmEvent::ConnectionRequest, event.get_event());

        // New connection established! Use this  connection for RDMA communication.
        let connected_id = event.get_connection_request_id();
        let client_private_data: PeerConnectionData<u64, 1> =
            event.get_private_data().expect("Missing private data!");
        event.ack();

        let mut pd = connected_id.allocate_protection_domain().expect("TODO");
        let cq = connected_id.create_cq().expect("TODO");
        let qp = connected_id.create_qp(&pd, &cq);

        // Now send our connection data to client.
        let mut recv_window = VolatileRdmaMemory::new(&mut pd);

        // dbg!(our_private_data);
        connected_id
            .accept_with_private_data(&recv_window.as_connection_data())
            .expect("TODO");
        let event = qd.cm.get_cm_event().await.expect("TODO");
        assert_eq!(RdmaCmEvent::Established, event.get_event());
        event.ack();

        let control_flow = ControlFlow::new(
            qp.clone(),
            pd.allocate_memory(),
            recv_window,
            client_private_data,
        );
        let scheduler_handle = self.executor.add_new_connection(control_flow, qp, pd, cq);

        QueueDescriptor {
            cm: connected_id,
            scheduler_handle: Some(scheduler_handle),
        }
    }
}
