use hashbrown::HashMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use structopt::StructOpt;

use io_queue_rdma;
use io_queue_rdma::{CompletedRequest, IoQueue, QueueDescriptor, QueueToken};
use nix::sys::socket::{InetAddr, SockAddr};
use rdma_cm::RdmaMemory;
use std::convert::TryInto;
use tracing_subscriber::EnvFilter;
use utilities::Statistics;

#[derive(StructOpt)]
#[structopt(about = "SOSP Echo Benchmark")]
enum Options {
    Server {
        #[structopt(long)]
        ip_address: String,
        #[structopt(long)]
        port: String,
    },
    Client {
        #[structopt(long)]
        ip_address: String,
        #[structopt(long)]
        port: String,
        #[structopt(long)]
        bufsize: usize,
        #[structopt(long)]
        nflows: usize,
    },
}

const RECV_WRS: usize = 4096;
const SEND_WRS: usize = 128;
const CQ_ELEMENTS: usize = 64;
const BUFFER_SIZE: usize = 1024;
const WINDOW_SIZE: usize = 1024;

fn main() {
    let options = Options::from_args();

    tracing_subscriber::fmt::Subscriber::builder()
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(false)
        .without_time()
        .init();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    match options {
        Options::Server { ip_address, port } => {
            let address = format!("{}:{}", ip_address, port);
            let address: SocketAddr = address.parse().expect("Unable to parse socket address");

            let mut server =
                Server::<RECV_WRS, SEND_WRS, CQ_ELEMENTS, WINDOW_SIZE, BUFFER_SIZE>::new(address);
            rt.block_on(server.run())
        }
        Options::Client {
            ip_address,
            port,
            bufsize,
            nflows,
        } => {
            let mut c = rt.block_on(Client::<
                RECV_WRS,
                SEND_WRS,
                CQ_ELEMENTS,
                WINDOW_SIZE,
                BUFFER_SIZE,
            >::new(bufsize, &ip_address, &port, nflows));
            c.client()
        }
    }
}

struct Server<
    const RECV_WRS: usize,
    const SEND_WRS: usize,
    const CQ_ELEMENTS: usize,
    const WINDOW_SIZE: usize,
    const BUFFER_SIZE: usize,
> {
    stats: Statistics,
    listening_qd: QueueDescriptor<false>,
    libos: IoQueue<RECV_WRS, SEND_WRS, CQ_ELEMENTS, WINDOW_SIZE, BUFFER_SIZE, false>,
}

impl<
        const RECV_WRS: usize,
        const SEND_WRS: usize,
        const CQ_ELEMENTS: usize,
        const WINDOW_SIZE: usize,
        const BUFFER_SIZE: usize,
    > Server<RECV_WRS, SEND_WRS, CQ_ELEMENTS, WINDOW_SIZE, BUFFER_SIZE>
{
    pub fn new(socket_address: SocketAddr) -> Self {
        let address = SockAddr::new_inet(InetAddr::from_std(&socket_address));

        let mut libos = IoQueue::new().set_async();
        // Setup connection.
        let mut listening_qd: QueueDescriptor<false> = libos.socket();
        libos.bind(&mut listening_qd, &address).unwrap();
        libos.listen(&mut listening_qd);

        Self {
            stats: Statistics::new("server"),
            listening_qd,
            libos,
        }
    }

    pub async fn run(&mut self) {
        let mut qtokens: Vec<QueueToken> = Vec::with_capacity(10000);
        let mut connected_qd = self.libos.accept(&mut self.listening_qd).await;

        // let mut bufsize: usize = 0;
        // let mut start: Instant = Instant::now();
        let mut every_second_timer: Instant = Instant::now();
        let mut processed_packages: usize = 0;

        // Wait for client to write to us.
        let qt = self.libos.pop(&mut connected_qd);
        qtokens.push(qt);

        loop {
            // Dump statistics.
            if every_second_timer.elapsed() > Duration::from_secs(1) {
                self.libos.get_and_reset_time();
                dbg!(&processed_packages);
                println!(
                    "{} gbps",
                    (processed_packages * 2 * 1024 * 8) as f64 / (1_000_000_000) as f64
                );
                processed_packages = 0;
                every_second_timer = Instant::now();
            }

            let (i, result) = self.libos.wait_any(&qtokens);
            qtokens.swap_remove(i);

            match result {
                CompletedRequest::Pop(memory) => {
                    // bufsize = memory.accessed();
                    let qt = self.libos.push(&mut connected_qd, memory);
                    qtokens.push(qt);
                    processed_packages += 1;

                    // Allow other pop if available for us to serve.
                    let qt = self.libos.pop(&mut connected_qd);
                    qtokens.push(qt);
                }
                CompletedRequest::Push(memory) => {
                    self.libos.free(&mut connected_qd, memory);
                }
            }
        }
    }
}

struct Client<
    const RECV_WRS: usize,
    const SEND_WRS: usize,
    const CQ_ELEMENTS: usize,
    const WINDOW_SIZE: usize,
    const N: usize,
> {
    stats: Statistics,
    qd: QueueDescriptor<false>,
    nflows: usize,
    bufsize: usize,
    libos: IoQueue<RECV_WRS, SEND_WRS, CQ_ELEMENTS, WINDOW_SIZE, N, false>,
    nextpkt: u64,
}

impl<
        const RECV_WRS: usize,
        const SEND_WRS: usize,
        const CQ_ELEMENTS: usize,
        const WINDOW_SIZE: usize,
        const N: usize,
    > Client<RECV_WRS, SEND_WRS, CQ_ELEMENTS, WINDOW_SIZE, N>
{
    pub async fn new(bufsize: usize, address: &str, port: &str, nflows: usize) -> Self {
        println!("buffer size {:?}, number of flows {:?}", bufsize, nflows);

        // Setup connection.
        let mut libos = IoQueue::new().set_async();
        let mut connection: QueueDescriptor<false> = libos.socket();

        libos.connect(&mut connection, &address, &port).await;

        Self {
            stats: Statistics::new("client"),
            qd: connection,
            nflows,
            bufsize,
            libos,
            nextpkt: 0,
        }
    }

    fn client(&mut self) {
        // Preallocate to avoid heap allocation during measurements.
        let mut qtokens: Vec<QueueToken> = Vec::with_capacity(10000);
        let mut packet_times: HashMap<u64, Instant> = HashMap::with_capacity(10000);
        let mut last_log = Instant::now();
        let mut processed_packages: usize = 0;

        // Send initial packets.
        for _ in 0..self.nflows {
            let (buf, stamp) = self.makepkt(self.bufsize);
            packet_times.insert(stamp, Instant::now());

            let qt = self.libos.push(&mut self.qd, buf);
            qtokens.push(qt);
            let qt = self.libos.pop(&mut self.qd);
            qtokens.push(qt);
        }

        loop {
            // Dump statistics.
            if last_log.elapsed() > Duration::from_secs(1) {
                self.stats.print();

                let total_io = self.libos.get_and_reset_time() as u64;
                println!(
                    "libOS time ({:?}) / processed_packages ({:?}) = {:?}",
                    Duration::from_nanos(total_io as u64),
                    processed_packages,
                    Duration::from_nanos((total_io as f64 / processed_packages as f64) as u64),
                );
                processed_packages = 0;
                last_log = Instant::now();
            }

            let (i, result) = self.libos.wait_any(&qtokens);
            qtokens.swap_remove(i);

            match result {
                CompletedRequest::Push(memory) => {
                    self.libos.free(&mut self.qd, memory);
                }
                CompletedRequest::Pop(memory) => {
                    // Record statistics.
                    let bufsize: usize = memory.accessed();
                    let stamp: u64 = Self::getstamp(&memory);
                    self.libos.free(&mut self.qd, memory);

                    let timestamp = packet_times.remove(&stamp).unwrap();
                    self.stats.record(2 * bufsize, timestamp.elapsed());

                    // Send another packet.
                    let (buf, stamp) = self.makepkt(self.bufsize);
                    packet_times.insert(stamp, Instant::now());

                    let qt = self.libos.push(&mut self.qd, buf);
                    qtokens.push(qt);

                    let qt = self.libos.pop(&mut self.qd);
                    qtokens.push(qt);
                    processed_packages += 1;
                }
            }
        }
    }

    fn makepkt(&mut self, pktsize: usize) -> (RdmaMemory<u8, N>, u64) {
        let mut rdma_memory = self.libos.malloc(&mut self.qd);

        assert!(pktsize <= N, "pktsize {}, pktbuf size {}", pktsize, N,);

        let stamp = self.nextpkt;
        self.nextpkt += 1;

        // Factory packet.
        let stamp_slice = stamp.to_ne_bytes();

        // Even though we are only writing to the first 8 bytes. We borrow up to `bufsize` to
        // ensure our RDMA implemetentation sends `bufsize` bytes over the network.
        let memory_slice = rdma_memory.as_mut_slice(self.bufsize);

        for i in 0..stamp_slice.len() {
            memory_slice[i] = stamp_slice[i];
        }

        (rdma_memory, stamp)
    }

    fn getstamp(memory: &RdmaMemory<u8, N>) -> u64 {
        u64::from_ne_bytes(memory.as_slice()[0..8].try_into().unwrap())
    }
}
