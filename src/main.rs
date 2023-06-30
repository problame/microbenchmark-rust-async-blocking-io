use std::os::fd::AsFd;
use std::{
    alloc::Layout,
    io::Write,
    num::NonZeroU64,
    os::{
        fd::{FromRawFd, IntoRawFd},
        unix::prelude::{FileExt, OpenOptionsExt},
    },
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
};

use clap::Parser;
use rand::{Rng, RngCore};
use tracing::{error, info, trace};

#[derive(clap::Parser)]
struct Args {
    num_clients: NonZeroU64,
    file_size_mib: NonZeroU64,
    block_size_shift: NonZeroU64,
    #[clap(subcommand)]
    engine: EngineKind,
}

#[derive(Clone, Copy, clap::Subcommand)]
enum EngineKind {
    Std,
    TokioSpawnBlocking,
    TokioFlume { workers: NonZeroU64 },
}

struct EngineStd {}

struct EngineTokioSpawnBlocking {
    rt: tokio::runtime::Runtime,
}

trait Engine {
    fn run(
        self: Arc<Self>,
        args: &'static Args,
        stop: Arc<AtomicBool>,
        reads_in_last_second: Arc<AtomicU64>,
    );
}

fn main() {
    std::env::set_var("RUST_LOG", "info");

    tracing_subscriber::fmt::init();

    let args: &'static Args = Box::leak(Box::new(Args::parse()));

    let stop = Arc::new(AtomicBool::new(false));

    setup_files(&args);

    let engine = setup_engine(&args);

    let reads_in_last_second = Arc::new(AtomicU64::new(0));

    ctrlc::set_handler({
        let stop = Arc::clone(&stop);
        move || {
            stop.store(true, Ordering::Relaxed);
        }
    });

    let monitor = std::thread::spawn({
        let stop = Arc::clone(&stop);
        let reads_in_last_second = Arc::clone(&reads_in_last_second);
        move || {
            while !stop.load(Ordering::Relaxed) {
                std::thread::sleep(std::time::Duration::from_secs(1));
                let reads_in_last_second = reads_in_last_second.swap(0, Ordering::Relaxed);
                info!(
                    "IOPS {}k BANDWIDTH {} MiB/s",
                    reads_in_last_second,
                    (1 << args.block_size_shift.get()) * reads_in_last_second / (1 << 20),
                );
            }
        }
    });

    engine.run(&args, stop, reads_in_last_second);
    monitor.join().unwrap();
}

fn data_dir(args: &Args) -> PathBuf {
    std::path::PathBuf::from("data")
}
fn data_file_path(args: &Args, client_num: u64) -> PathBuf {
    std::path::PathBuf::from("data").join(format!("client_{}.data", client_num))
}

fn setup_files(args: &Args) {
    let data_dir = data_dir(args);
    std::fs::create_dir_all(&data_dir).unwrap();
    for i in 0..args.num_clients.get() {
        let file_path = data_file_path(args, i);
        match std::fs::metadata(&file_path) {
            Ok(md) => {
                if md.len() == args.file_size_mib.get() * 1024 * 1024 {
                    continue;
                } else {
                    info!("File {:?} exists but has wrong size", file_path);
                    std::fs::remove_file(&file_path).unwrap();
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => panic!("Error while checking file {:?}: {}", file_path, e),
        }
        let mut file = std::fs::File::create(&file_path).unwrap();
        // fill the file with pseudo-random data, in 1 MiB chunks.
        for _ in 0..args.file_size_mib.get() {
            let mut chunk = [0u8; 1024 * 1024];
            rand::thread_rng().fill_bytes(&mut chunk);
            file.write_all(&chunk).unwrap();
        }
    }
    // assert invariant
    for i in 0..args.num_clients.get() {
        let file_path = data_dir.join(&format!("client_{}.data", i));
        let md = std::fs::metadata(&file_path).unwrap();
        assert_eq!(md.len(), args.file_size_mib.get() * 1024 * 1024);
    }
}

fn setup_engine(args: &Args) -> Arc<dyn Engine> {
    match args.engine {
        EngineKind::Std => Arc::new(EngineStd {}),
        EngineKind::TokioSpawnBlocking => {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();
            Arc::new(EngineTokioSpawnBlocking { rt })
        }
        EngineKind::TokioFlume { workers } => {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();
            Arc::new(EngineTokioFlume {
                rt,
                num_workers: workers,
            })
        }
    }
}

impl Engine for EngineStd {
    fn run(
        self: Arc<Self>,
        args: &'static Args,
        stop: Arc<AtomicBool>,
        reads_in_last_second: Arc<AtomicU64>,
    ) {
        std::thread::scope(|scope| {
            for i in 0..args.num_clients.get() {
                let stop = Arc::clone(&stop);
                let reads_in_last_second = Arc::clone(&reads_in_last_second);
                let myself = Arc::clone(&self);
                scope.spawn(move || myself.client(i, &args, &stop, &reads_in_last_second));
            }
        });
    }
}
impl EngineStd {
    fn client(&self, i: u64, args: &Args, stop: &AtomicBool, reads_in_last_second: &AtomicU64) {
        tracing::info!("Client {i} starting");
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECT)
            .open(data_file_path(args, i))
            .unwrap();
        let block_size = 1 << args.block_size_shift.get();
        // alloc aligned to make O_DIRECT work
        let buf =
            unsafe { std::alloc::alloc(Layout::from_size_align(block_size, block_size).unwrap()) };
        assert!(!buf.is_null());
        let buf: &mut [u8] = unsafe { std::slice::from_raw_parts_mut(buf, block_size) };
        let block_size: u64 = block_size.try_into().unwrap();
        while !stop.load(Ordering::Relaxed) {
            // find a random aligned 8k offset inside the file
            debug_assert!(1024 * 1024 % block_size == 0);
            let offset_in_file = rand::thread_rng()
                .gen_range(0..=((args.file_size_mib.get() * 1024 * 1024 - 1) / block_size))
                * block_size;
            self.read_iter(i, args, &mut file, offset_in_file, buf);
            reads_in_last_second.fetch_add(1, Ordering::Relaxed);
        }
        info!("Client {i} stopping");
    }

    #[inline(always)]
    fn read_iter(
        &self,
        client_num: u64,
        args: &Args,
        file: &mut std::fs::File,
        offset: u64,
        buf: &mut [u8],
    ) {
        debug_assert_eq!(buf.len(), args.block_size_shift.get() as usize);
        file.read_at(buf, offset).unwrap();
        // TODO: verify
    }
}

impl Engine for EngineTokioSpawnBlocking {
    fn run(
        self: Arc<Self>,
        args: &'static Args,
        stop: Arc<AtomicBool>,
        reads_in_last_second: Arc<AtomicU64>,
    ) {
        let rt = &self.rt;
        rt.block_on(async {
            let mut handles = Vec::new();
            for i in 0..args.num_clients.get() {
                let stop = Arc::clone(&stop);
                let reads_in_last_second = Arc::clone(&reads_in_last_second);
                handles.push(tokio::spawn(async move {
                    Self::client(i, &args, &stop, &reads_in_last_second).await
                }));
            }
            for handle in handles {
                handle.await.unwrap();
            }
        });
    }
}

impl EngineTokioSpawnBlocking {
    async fn client(i: u64, args: &Args, stop: &AtomicBool, reads_in_last_second: &AtomicU64) {
        tracing::info!("Client {i} starting");
        let block_size = 1 << args.block_size_shift.get();
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECT)
            .open(data_file_path(args, i))
            .unwrap();
        let file_fd = file.into_raw_fd();

        // alloc aligned to make O_DIRECT work
        let buf = {
            let buf_ptr = unsafe {
                std::alloc::alloc(Layout::from_size_align(block_size, block_size).unwrap())
            };
            assert!(!buf_ptr.is_null());
            #[derive(Clone, Copy)]
            struct SendPtr(*mut u8);
            unsafe impl Send for SendPtr {} // the thread spawned in the loop below doesn't outlive this function (we're polled t completion)
            unsafe impl Sync for SendPtr {} // the loop below ensures only one thread accesses it at a time
            let buf = SendPtr(buf_ptr);
            // extra scope so that it doesn't outlive any await points, it's not Send, only the SendPtr wrapper is
            buf
        };
        let block_size_u64: u64 = block_size.try_into().unwrap();
        while !stop.load(Ordering::Relaxed) {
            // find a random aligned 8k offset inside the file
            debug_assert!(1024 * 1024 % block_size == 0);
            let offset_in_file = rand::thread_rng()
                .gen_range(0..=((args.file_size_mib.get() * 1024 * 1024 - 1) / block_size_u64))
                * block_size_u64;
            tokio::task::spawn_blocking(move || {
                let buf = buf;
                let buf: &mut [u8] = unsafe { std::slice::from_raw_parts_mut(buf.0, block_size) };
                let file = unsafe { std::fs::File::from_raw_fd(file_fd) };
                file.read_at(buf, offset_in_file).unwrap();
                file.into_raw_fd(); // so that it's there for next iteration
            })
            .await
            .unwrap();
            reads_in_last_second.fetch_add(1, Ordering::Relaxed);
        }
        info!("Client {i} stopping");
    }
}

struct EngineTokioFlume {
    rt: tokio::runtime::Runtime,
    num_workers: NonZeroU64,
}

impl Engine for EngineTokioFlume {
    fn run(
        self: Arc<Self>,
        args: &'static Args,
        stop: Arc<AtomicBool>,
        reads_in_last_second: Arc<AtomicU64>,
    ) {
        let (worker_tx, work_rx) = flume::bounded(0);
        let mut handles = Vec::new();
        for _ in 0..self.num_workers.get() {
            let work_rx = work_rx.clone();
            let stop = Arc::clone(&stop);
            let reads_in_last_second = Arc::clone(&reads_in_last_second);
            let handle = std::thread::spawn(move || Self::worker(work_rx));
            handles.push(handle);
        }
        // workers get stopped by the worker_tx being dropped
        scopeguard::defer!(for handle in handles {
            handle.join().unwrap();
        });
        let rt = &self.rt;
        rt.block_on(async {
            let mut handles = Vec::new();
            for i in 0..args.num_clients.get() {
                let stop = Arc::clone(&stop);
                let reads_in_last_second = Arc::clone(&reads_in_last_second);
                let worker_tx = worker_tx.clone();
                let myself = Arc::clone(&self);
                handles.push(tokio::spawn(async move {
                    myself
                        .client(worker_tx, i, &args, &stop, &reads_in_last_second)
                        .await
                }));
            }
            for handle in handles {
                handle.await.unwrap();
            }
        });
        drop(worker_tx); // stops the workers
    }
}

type FlumeWork = Box<dyn FnOnce() -> std::io::Result<()> + Send + 'static>;

struct FlumeWorkRequest {
    work: FlumeWork,
    response: tokio::sync::oneshot::Sender<std::io::Result<()>>,
}

impl EngineTokioFlume {
    async fn client(
        &self,
        worker_tx: flume::Sender<FlumeWorkRequest>,
        i: u64,
        args: &Args,
        stop: &AtomicBool,
        reads_in_last_second: &AtomicU64,
    ) {
        tracing::info!("Client {i} starting");
        let block_size = 1 << args.block_size_shift.get();
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECT)
            .open(data_file_path(args, i))
            .unwrap();
        let file_fd = file.into_raw_fd();

        // alloc aligned to make O_DIRECT work
        let buf = {
            let buf_ptr = unsafe {
                std::alloc::alloc(Layout::from_size_align(block_size, block_size).unwrap())
            };
            assert!(!buf_ptr.is_null());
            #[derive(Clone, Copy)]
            struct SendPtr(*mut u8);
            unsafe impl Send for SendPtr {} // the thread spawned in the loop below doesn't outlive this function (we're polled t completion)
            unsafe impl Sync for SendPtr {} // the loop below ensures only one thread accesses it at a time
            let buf = SendPtr(buf_ptr);
            // extra scope so that it doesn't outlive any await points, it's not Send, only the SendPtr wrapper is
            buf
        };
        let block_size_u64: u64 = block_size.try_into().unwrap();
        while !stop.load(Ordering::Relaxed) {
            // find a random aligned 8k offset inside the file
            debug_assert!(1024 * 1024 % block_size == 0);
            let offset_in_file = rand::thread_rng()
                .gen_range(0..=((args.file_size_mib.get() * 1024 * 1024 - 1) / block_size_u64))
                * block_size_u64;
            let work = Box::new(move || {
                let buf = buf;
                let buf: &mut [u8] = unsafe { std::slice::from_raw_parts_mut(buf.0, block_size) };
                let file = unsafe { std::fs::File::from_raw_fd(file_fd) };
                file.read_at(buf, offset_in_file).unwrap();
                file.into_raw_fd(); // so that it's there for next iteration
                Ok(())
            });
            // TODO: can this dealock with rendezvous channel?
            let (response_tx, response_rx) = tokio::sync::oneshot::channel();
            worker_tx
                .send_async(FlumeWorkRequest {
                    work,
                    response: response_tx,
                })
                .await;
            response_rx
                .await
                .expect("rx flume")
                .expect("not expecting io errors");
            reads_in_last_second.fetch_add(1, Ordering::Relaxed);
        }
        info!("Client {i} stopping");
    }
    fn worker(rx: flume::Receiver<FlumeWorkRequest>) {
        loop {
            let FlumeWorkRequest { work, response } = match rx.recv() {
                Ok(w) => w,
                Err(flume::RecvError::Disconnected) => {
                    info!("Worker stopping");
                    return;
                }
            };
            let res = work();
            match response.send(res) {
                Ok(()) => (),
                Err(x) => {
                    error!("Failed to send response: {:?}", x);
                }
            }
        }
    }
}
