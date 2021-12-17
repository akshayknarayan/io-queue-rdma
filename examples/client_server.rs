use io_queue_rdma::IoQueue;
use nix::sys::socket::{InetAddr, SockAddr};
use std::net::SocketAddr;
use std::str::FromStr;
use structopt::StructOpt;
use tracing_subscriber::EnvFilter;

#[derive(Debug)]
enum Mode {
    Client,
    Server,
}

impl FromStr for Mode {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Client" | "client" => Ok(Mode::Client),
            "Server" | "server" => Ok(Mode::Server),
            _ => Err("Unknown mode. Available modes: 'client', 'server'."),
        }
    }
}

#[derive(Debug, StructOpt)]
#[structopt(
    name = "IoQueue Client/Server",
    about = "Example IOQueue RDMA Client/Server Program."
)]
struct Opt {
    #[structopt(short, long)]
    mode: Mode,
    #[structopt(short, long)]
    ip_address: String,
    #[structopt(short, long)]
    port: String,
}

fn main() {
    tracing_subscriber::fmt::Subscriber::builder()
        .with_env_filter(EnvFilter::from_default_env())
        .with_target(false)
        .without_time()
        .init();

    let opt = Opt::from_args();

    let address = format!("{}:{}", opt.ip_address, opt.port);
    let address: SocketAddr = address.parse().expect("Unable to parse socket address");

    match opt.mode {
        Mode::Server => {
            let mut io_queue = IoQueue::<2048, 256, 32, 1024, 1024, true>::new();
            let mut listening_qd = io_queue.socket();

            io_queue
                .bind(
                    &mut listening_qd,
                    &SockAddr::new_inet(InetAddr::from_std(&address)),
                )
                .unwrap();
            io_queue.listen(&mut listening_qd);
            let mut connected_qd = io_queue.accept(&mut listening_qd);
            println!("Connected to client!");

            println!("Waiting to receive byte...");
            let qt = io_queue.pop(&mut connected_qd);
            let mut buffer = io_queue.wait(qt).pop_op();

            println!("Server got: {:?}", buffer.as_mut_slice(1)[0]);
        }
        Mode::Client => {
            let mut io_queue = IoQueue::<2048, 256, 32, 1024, 1024, true>::new();
            let mut connection = io_queue.socket();
            io_queue.connect(&mut connection, &opt.ip_address, &opt.port);

            println!("Sending byte to server.");
            let mut memory = io_queue.malloc(&mut connection);
            memory.as_mut_slice(1)[0] = 42;
            let qt = io_queue.push(&mut connection, memory);
            // Acquire our allocated memory again.
            let memory = io_queue.wait(qt).push_op();
            io_queue.free(&mut connection, memory);
            println!("Byte sent!");
        }
    }
}
