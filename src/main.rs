use std::{
    alloc::Layout,
    collections::HashMap,
    io::{Seek, Write},
    num::NonZeroU64,
    os::{
        fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd, RawFd},
        unix::prelude::FileExt,
    },
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use clap::Parser;
use crossbeam_utils::CachePadded;
use itertools::Itertools;
use rand::{Rng, RngCore};
use tracing::{error, info};

#[derive(clap::Parser)]
struct Args {
    num_clients: NonZeroU64,
    file_size_mib: NonZeroU64,
    block_size_shift: NonZeroU64,
    #[clap(subcommand)]
    work_kind: WorkKind,
}

#[derive(Clone, Copy, clap::Subcommand)]
enum WorkKind {
    DiskAccess {
        #[clap(subcommand)]
        disk_access_kind: DiskAccessKind,
    },
    TimerFd {
        #[clap(subcommand)]
        expiration_mode: TimerFdExperiationModeKind,
    },
    NoWork {
        #[clap(subcommand)]
        engine: EngineKind,
    },
}

#[derive(Clone, Copy, clap::Subcommand)]
enum TimerFdExperiationModeKind {
    Oneshot {
        micros: NonZeroU64,
        #[clap(subcommand)]
        engine: EngineKind,
    },
}

impl WorkKind {
    fn engine(&self) -> &EngineKind {
        match self {
            WorkKind::TimerFd { expiration_mode } => match expiration_mode {
                TimerFdExperiationModeKind::Oneshot { engine, .. } => engine,
            },
            WorkKind::DiskAccess { disk_access_kind } => match disk_access_kind {
                DiskAccessKind::DirectIo { engine } => engine,
                DiskAccessKind::CachedIo { engine } => engine,
            },
            WorkKind::NoWork { engine } => engine,
        }
    }
}

#[derive(Copy, Clone, clap::Subcommand)]
enum DiskAccessKind {
    DirectIo {
        #[clap(subcommand)]
        engine: EngineKind,
    },
    CachedIo {
        #[clap(subcommand)]
        engine: EngineKind,
    },
}

#[derive(Clone, Copy, clap::Subcommand)]
enum EngineKind {
    Std,
    TokioOnExecutorThread,
    TokioSpawnBlocking {
        spawn_blocking_pool_size: NonZeroU64,
    },
    TokioFlume {
        workers: NonZeroU64,
        queue_depth: NonZeroU64,
    },
    TokioRio {
        mode: TokioRioModeKind,
    },
    TokioIoUringEventfdBridge,
}

struct EngineStd {}

struct EngineTokioRio {
    rt: tokio::runtime::Runtime,
    mode: TokioRioMode,
}

#[derive(Clone, Copy, clap::ValueEnum)]
enum TokioRioModeKind {
    SingleGlobal,
    ExecutorThreadLocal,
    Epoll,
    EpollExecutorThreadLocal,
}

enum TokioRioMode {
    SingleGlobal(Arc<rio::Rio>),
    ExecutorThreadLocal,
    Epoll {
        rio: Arc<rio::Rio>,
        reaper: Option<rio::Reaper>,
    },
    EpollExecutorThreadLocal,
}

#[derive(Clone)]
enum TokioRioModeReduced {
    SingleGlobal(Arc<rio::Rio>),
    ExecutorThreadLocal {
        track_thread_local_rio: TrackThreadLocalRioFn,
    },
    Epoll(Arc<rio::Rio>),
    EpollExecutorThreadLocal {
        track_thread_local_rio: TrackEpollThreadLocalRioFn,
    },
}

type ThreadLocalRio = Arc<Mutex<Option<Arc<rio::Rio>>>>;
type TrackThreadLocalRioFn = Arc<dyn Fn(&ThreadLocalRio) + Send + Sync>;
type TrackEpollThreadLocalRioFn = Arc<dyn Fn(&ThreadLocalRio, rio::Reaper) + Send + Sync>;
type LaunchEventfdPollerFn = Arc<dyn Fn(&rio::Rio, rio::Reaper) + Send + Sync>;

struct EngineTokioSpawnBlocking {
    rt: tokio::runtime::Runtime,
}

struct StatsState {
    reads_in_last_second: Vec<crossbeam_utils::CachePadded<AtomicU64>>,
    client0_latency_sample: AtomicU64,
    tokio_rio_epoll_iterations: AtomicU64,
}

trait Engine {
    fn run(
        self: Box<Self>,
        args: &'static Args,
        works: Vec<ClientWork>,
        stop: Arc<AtomicBool>,
        reads_in_last_second: Arc<StatsState>,
    );
}

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter({
            tracing_subscriber::EnvFilter::try_from_default_env()
                .expect("must set RUST_LOG variable")
        })
        .init();

    let args: &'static Args = Box::leak(Box::new(Args::parse()));

    let stop = Arc::new(AtomicBool::new(false));

    let works = setup_client_works(&args);

    let engine = setup_engine(&args.work_kind.engine());

    let stats_state = Arc::new(StatsState {
        reads_in_last_second: (0..works.len())
            .into_iter()
            .map(|_| CachePadded::new(AtomicU64::new(0)))
            .collect(),
        client0_latency_sample: AtomicU64::new(0),
        tokio_rio_epoll_iterations: AtomicU64::new(0),
    });

    ctrlc::set_handler({
        let stop = Arc::clone(&stop);
        move || {
            if stop.fetch_or(true, Ordering::Relaxed) {
                error!("Received second SIGINT, aborting");
                std::process::abort();
            }
        }
    })
    .unwrap();

    let monitor = std::thread::spawn({
        let stop = Arc::clone(&stop);
        let stats_state = Arc::clone(&stats_state);

        move || {
            let mut per_task_total_reads = HashMap::new();

            while !stop.load(Ordering::Relaxed) {
                std::thread::sleep(std::time::Duration::from_secs(1));
                let mut reads_in_last_second = 0;
                for (client, counter) in stats_state.reads_in_last_second.iter().enumerate() {
                    let task_reads_in_last_second = counter.swap(0, Ordering::Relaxed);
                    let per_task = per_task_total_reads.entry(client).or_insert(0);
                    *per_task += task_reads_in_last_second;
                    reads_in_last_second += task_reads_in_last_second;
                }
                let client0_latency_sample = stats_state
                    .client0_latency_sample
                    .swap(0, Ordering::Relaxed);
                let tokio_rio_epoll_iterations = stats_state
                    .tokio_rio_epoll_iterations
                    .swap(0, Ordering::Relaxed);
                info!(
                    "IOPS {} LatClient0 {}us BANDWIDTH {} MiB/s",
                    reads_in_last_second,
                    client0_latency_sample,
                    (1 << args.block_size_shift.get()) * reads_in_last_second / (1 << 20),
                );
                match args.work_kind.engine() {
                    EngineKind::TokioRio { mode } => match mode {
                        TokioRioModeKind::Epoll | TokioRioModeKind::EpollExecutorThreadLocal => {
                            if tokio_rio_epoll_iterations < 10 {
                                tracing::warn!(
                                    "Tokio RIO epoll iterations {}",
                                    tokio_rio_epoll_iterations
                                );
                            }
                        }
                        _ => {}
                    },
                    _ => {}
                }
            }

            // dump per-task total reads into a json file.
            // Useful to judge fairness (i.e., did each client task get about the same nubmer of ops in a time-based run).
            //
            // command line to get something for copy-paste into google sheets:
            //  ssh neon-devvm-mbp ssh testinstance sudo cat /mnt/per_task_total_reads.json | jq '.[]' | pbcopy
            let sorted_per_task_total_reads = per_task_total_reads
                .values()
                .cloned()
                .sorted()
                .map(|v| v as f64)
                .collect::<Vec<f64>>();
            std::fs::write(
                "per_task_total_reads.json",
                serde_json::to_string(&sorted_per_task_total_reads).unwrap(),
            )
            .unwrap();
        }
    });

    engine.run(&args, works, stop, stats_state);
    monitor.join().unwrap();
}

enum ClientWork {
    DiskAccess {
        file: std::fs::File,
    },
    TimerFdSetStateAndRead {
        timerfd: timerfd::TimerFd,
        duration: Duration,
    },
    NoWork {},
}

fn setup_client_works(args: &Args) -> Vec<ClientWork> {
    match &args.work_kind {
        WorkKind::DiskAccess { disk_access_kind } => {
            setup_files(&args, disk_access_kind);
            // assert invariant and open files
            let mut client_files = Vec::new();
            for i in 0..args.num_clients.get() {
                let file_path = data_file_path(args, i);
                let md = std::fs::metadata(&file_path).unwrap();
                assert!(md.len() >= args.file_size_mib.get() * 1024 * 1024);

                let file = open_file_direct_io(
                    disk_access_kind,
                    OpenFileMode::Read,
                    &data_file_path(args, i),
                );
                client_files.push(ClientWork::DiskAccess { file });
            }
            client_files
        }
        WorkKind::TimerFd { expiration_mode } => (0..args.num_clients.get())
            .map(|_| match expiration_mode {
                TimerFdExperiationModeKind::Oneshot { micros, engine: _ } => {
                    ClientWork::TimerFdSetStateAndRead {
                        timerfd: {
                            timerfd::TimerFd::new_custom(timerfd::ClockId::Monotonic, false, true)
                                .unwrap()
                        },
                        duration: Duration::from_micros(micros.get()),
                    }
                }
            })
            .collect(),
        WorkKind::NoWork { engine: _ } => (0..args.num_clients.get())
            .map(|_| ClientWork::NoWork {})
            .collect(),
    }
}

fn data_dir(_args: &Args) -> PathBuf {
    std::path::PathBuf::from("data")
}
fn data_file_path(_args: &Args, client_num: u64) -> PathBuf {
    std::path::PathBuf::from("data").join(format!("client_{}.data", client_num))
}

fn alloc_self_aligned_buffer(size: usize) -> *mut u8 {
    let buf_ptr = unsafe { std::alloc::alloc(Layout::from_size_align(size, size).unwrap()) };
    assert!(!buf_ptr.is_null());
    buf_ptr
}

fn setup_files(args: &Args, disk_access_kind: &DiskAccessKind) {
    let data_dir = data_dir(args);
    std::fs::create_dir_all(&data_dir).unwrap();
    std::thread::scope(|scope| {
        for i in 0..args.num_clients.get() {
            let file_path = data_file_path(args, i);
            let (append_offset, append_megs) = match std::fs::metadata(&file_path) {
                Ok(md) => {
                    if md.len() >= args.file_size_mib.get() * 1024 * 1024 {
                        (0, 0)
                    } else {
                        info!("File {:?} exists but has wrong size", file_path);
                        let rounded_down_megs = md.len() / (1024 * 1024);
                        let rounded_down_offset = rounded_down_megs * 1024 * 1024;
                        let append_megs = args
                            .file_size_mib
                            .get()
                            .checked_sub(rounded_down_megs)
                            .unwrap();
                        (rounded_down_offset, append_megs)
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => (0, args.file_size_mib.get()),
                Err(e) => panic!("Error while checking file {:?}: {}", file_path, e),
            };
            if append_megs == 0 {
                continue;
            }
            let mut file =
                open_file_direct_io(disk_access_kind, OpenFileMode::WriteNoTruncate, &file_path);
            file.seek(std::io::SeekFrom::Start(append_offset)).unwrap();

            // fill the file with pseudo-random data
            scope.spawn(move || {
                let chunk = alloc_self_aligned_buffer(1 << 20);
                let chunk = unsafe { std::slice::from_raw_parts_mut(chunk, 1 << 20) };
                for _ in 0..append_megs {
                    rand::thread_rng().fill_bytes(chunk);
                    file.write_all(&chunk).unwrap();
                }
            });
        }
    });
}

fn setup_engine(engine_kind: &EngineKind) -> Box<dyn Engine> {
    match engine_kind {
        EngineKind::Std => Box::new(EngineStd {}),
        EngineKind::TokioOnExecutorThread => {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();
            Box::new(EngineTokioOnExecutorThread { rt })
        }
        EngineKind::TokioSpawnBlocking {
            spawn_blocking_pool_size,
        } => {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .max_blocking_threads(spawn_blocking_pool_size.get() as usize)
                .build()
                .unwrap();
            Box::new(EngineTokioSpawnBlocking { rt })
        }
        EngineKind::TokioFlume {
            workers,
            queue_depth,
        } => {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();
            Box::new(EngineTokioFlume {
                rt,
                num_workers: *workers,
                queue_depth: *queue_depth,
            })
        }
        EngineKind::TokioRio { mode } => {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();
            // let rt = tokio::runtime::Builder::new_current_thread()
            //     .enable_all()
            //     .build()
            //     .unwrap();
            // rt.spawn(async move {
            //     loop {
            //         tokio::time::sleep(Duration::from_millis(100)).await;
            //         info!("tokio runtime is live");
            //     }
            // });
            Box::new(match mode {
                TokioRioModeKind::SingleGlobal => EngineTokioRio {
                    rt,
                    mode: TokioRioMode::SingleGlobal(Arc::new(rio::new().unwrap())),
                },
                TokioRioModeKind::ExecutorThreadLocal => EngineTokioRio {
                    rt,
                    mode: TokioRioMode::ExecutorThreadLocal,
                },
                TokioRioModeKind::Epoll => {
                    let (rio, reaper) = rio::Config::default().start(true).unwrap();
                    let reaper = reaper.unwrap();
                    EngineTokioRio {
                        rt,
                        mode: TokioRioMode::Epoll {
                            rio: Arc::new(rio),
                            reaper: Some(reaper),
                        },
                    }
                }
                TokioRioModeKind::EpollExecutorThreadLocal => EngineTokioRio {
                    rt,
                    mode: TokioRioMode::EpollExecutorThreadLocal,
                },
            })
        }
        EngineKind::TokioIoUringEventfdBridge => {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();
            Box::new(EngineTokioIoUringEventfdBridge { rt })
        }
    }
}

enum OpenFileMode {
    Read,
    WriteNoTruncate,
}

fn open_file_direct_io(
    disk_access_kind: &DiskAccessKind,
    mode: OpenFileMode,
    path: &Path,
) -> std::fs::File {
    let (read, write) = match mode {
        OpenFileMode::Read => (true, false),
        OpenFileMode::WriteNoTruncate => (false, true),
    };
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::prelude::OpenOptionsExt;
        let mut options = std::fs::OpenOptions::new();
        options.read(read).write(write).create(write);
        match disk_access_kind {
            DiskAccessKind::DirectIo { engine: _ } => {
                options.custom_flags(libc::O_DIRECT);
            }
            DiskAccessKind::CachedIo { engine: _ } => {}
        }
        options.open(path).unwrap()
    }

    // https://github.com/axboe/fio/issues/48
    // summarized in https://github.com/ronomon/direct-io/issues/1#issuecomment-360331547
    // => macOS does not support O_DIRECT, but we can use fcntl to set F_NOCACHE
    // If the file pages are in the page cache, this has no effect though.

    #[cfg(target_os = "macos")]
    {
        use std::os::unix::io::AsRawFd;
        let file = std::fs::OpenOptions::new()
            .read(read)
            .write(write)
            .create(write)
            .open(path)
            .unwrap();
        match args.direct_io {
            WorkKind::DirectIo => {
                let fd = file.as_raw_fd();
                let res = unsafe { libc::fcntl(fd, libc::F_NOCACHE, 1) };
                assert_eq!(res, 0);
            }
            WorkKind::CachedIo => {}
        }
        file
    }
}
impl Engine for EngineStd {
    fn run(
        self: Box<Self>,
        args: &'static Args,
        works: Vec<ClientWork>,
        stop: Arc<AtomicBool>,
        stats_state: Arc<StatsState>,
    ) {
        let myself = Arc::new(*self);
        std::thread::scope(|scope| {
            assert_eq!(works.len(), args.num_clients.get() as usize);
            for (i, work) in (0..args.num_clients.get()).zip(works.into_iter()) {
                let stop = Arc::clone(&stop);
                let stats_state = Arc::clone(&stats_state);
                let myself = Arc::clone(&myself);
                scope.spawn(move || myself.client(i, &args, work, &stop, stats_state));
            }
        });
    }
}
impl EngineStd {
    fn client(
        &self,
        i: u64,
        args: &Args,
        mut work: ClientWork,
        stop: &AtomicBool,
        stats_state: Arc<StatsState>,
    ) {
        tracing::info!("Client {i} starting");
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
            let start = std::time::Instant::now();
            match &mut work {
                ClientWork::DiskAccess { file } => {
                    self.read_iter(i, args, file, offset_in_file, buf);
                }
                ClientWork::TimerFdSetStateAndRead { timerfd, duration } => {
                    timerfd.set_state(
                        timerfd::TimerState::Oneshot(*duration),
                        timerfd::SetTimeFlags::Default,
                    );
                    timerfd.read();
                }
                ClientWork::NoWork {} => {}
            }
            stats_state.reads_in_last_second[usize::try_from(i).unwrap()]
                .fetch_add(1, Ordering::Relaxed);
            if i == 0 {
                stats_state
                    .client0_latency_sample
                    .store(start.elapsed().as_micros() as u64, Ordering::Relaxed);
            }
        }
        info!("Client {i} stopping");
    }

    #[inline(always)]
    fn read_iter(
        &self,
        _client_num: u64,
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

struct EngineTokioOnExecutorThread {
    rt: tokio::runtime::Runtime,
}

impl Engine for EngineTokioOnExecutorThread {
    fn run(
        self: Box<Self>,
        args: &'static Args,
        works: Vec<ClientWork>,
        stop: Arc<AtomicBool>,
        stats_state: Arc<StatsState>,
    ) {
        let rt = &self.rt;
        rt.block_on(async {
            let mut handles = Vec::new();
            assert_eq!(works.len(), args.num_clients.get() as usize);
            let all_client_tasks_spawned =
                Arc::new(tokio::sync::Barrier::new(args.num_clients.get() as usize));
            for (i, work) in (0..args.num_clients.get()).zip(works.into_iter()) {
                let stop = Arc::clone(&stop);
                let stats_state = Arc::clone(&stats_state);
                let all_client_tasks_spawned = Arc::clone(&all_client_tasks_spawned);
                handles.push(tokio::spawn(async move {
                    Self::client(i, &args, work, &stop, stats_state, all_client_tasks_spawned).await
                }));
            }
            for handle in handles {
                handle.await.unwrap();
            }
        });
    }
}

impl EngineTokioOnExecutorThread {
    async fn client(
        i: u64,
        args: &Args,
        mut work: ClientWork,
        stop: &AtomicBool,
        stats_state: Arc<StatsState>,
        all_client_tasks_spawned: Arc<tokio::sync::Barrier>,
    ) {
        tracing::info!("Client {i} starting");
        all_client_tasks_spawned.wait().await;
        let block_size = 1 << args.block_size_shift.get();

        let rwlock = Arc::new(tokio::sync::RwLock::new(()));
        std::mem::forget(Arc::clone(&rwlock));

        // alloc aligned to make O_DIRECT work
        let buf = {
            let buf_ptr = unsafe {
                std::alloc::alloc(Layout::from_size_align(block_size, block_size).unwrap())
            };
            assert!(!buf_ptr.is_null());
            let buf: &mut [u8] = unsafe { std::slice::from_raw_parts_mut(buf_ptr, block_size) };
            buf
        };
        let block_size_u64: u64 = block_size.try_into().unwrap();
        while !stop.load(Ordering::Relaxed) {
            // simulate Timeline::layers.read().await
            let _guard = rwlock.read().await;

            // find a random aligned 8k offset inside the file
            debug_assert!(1024 * 1024 % block_size == 0);
            let offset_in_file = rand::thread_rng()
                .gen_range(0..=((args.file_size_mib.get() * 1024 * 1024 - 1) / block_size_u64))
                * block_size_u64;
            let start = std::time::Instant::now();
            match &mut work {
                ClientWork::DiskAccess { file } => {
                    file.read_at(buf, offset_in_file).unwrap();
                }
                ClientWork::TimerFdSetStateAndRead { timerfd, duration } => {
                    timerfd.set_state(
                        timerfd::TimerState::Oneshot(*duration),
                        timerfd::SetTimeFlags::Default,
                    );
                    timerfd.read();
                }
                ClientWork::NoWork {} => {}
            }
            stats_state.reads_in_last_second[usize::try_from(i).unwrap()]
                .fetch_add(1, Ordering::Relaxed);
            // if i == 0 {
            // info!("Client {i} read took {:?}", start.elapsed());
            stats_state
                .client0_latency_sample
                .store(start.elapsed().as_micros() as u64, Ordering::Relaxed);
            // }
        }
        info!("Client {i} stopping");
    }
}

impl Engine for EngineTokioSpawnBlocking {
    fn run(
        self: Box<Self>,
        args: &'static Args,
        works: Vec<ClientWork>,
        stop: Arc<AtomicBool>,
        stats_state: Arc<StatsState>,
    ) {
        let rt = &self.rt;
        rt.block_on(async {
            let mut handles = Vec::new();
            assert_eq!(works.len(), args.num_clients.get() as usize);
            for (i, work) in (0..args.num_clients.get()).zip(works.into_iter()) {
                let stop = Arc::clone(&stop);
                let stats_state = Arc::clone(&stats_state);
                handles.push(tokio::spawn(async move {
                    Self::client(i, &args, work, &stop, stats_state).await
                }));
            }
            for handle in handles {
                handle.await.unwrap();
            }
        });
    }
}

impl EngineTokioSpawnBlocking {
    async fn client(
        i: u64,
        args: &Args,
        work: ClientWork,
        stop: &AtomicBool,
        stats_state: Arc<StatsState>,
    ) {
        tracing::info!("Client {i} starting");
        let block_size = 1 << args.block_size_shift.get();

        let rwlock = Arc::new(tokio::sync::RwLock::new(()));
        std::mem::forget(Arc::clone(&rwlock));

        #[derive(Copy, Clone)]
        enum ClientWorkFd {
            DiskAccess(RawFd),
            TimerFd(RawFd, Duration),
            NoWork,
        }

        let fd = match work {
            ClientWork::DiskAccess { file } => ClientWorkFd::DiskAccess(file.into_raw_fd()),
            ClientWork::TimerFdSetStateAndRead { timerfd, duration } => {
                let ret = ClientWorkFd::TimerFd(timerfd.as_raw_fd(), duration);
                std::mem::forget(timerfd); // they don't support into_raw_fd
                ret
            }
            ClientWork::NoWork {} => ClientWorkFd::NoWork,
        };

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
            // simulate Timeline::layers.read().await
            let _guard = rwlock.read().await;

            // find a random aligned 8k offset inside the file
            debug_assert!(1024 * 1024 % block_size == 0);
            let offset_in_file = rand::thread_rng()
                .gen_range(0..=((args.file_size_mib.get() * 1024 * 1024 - 1) / block_size_u64))
                * block_size_u64;
            let start = std::time::Instant::now();
            tokio::task::spawn_blocking(move || {
                match fd {
                    ClientWorkFd::DiskAccess(file_fd) => {
                        let buf = buf;
                        let buf: &mut [u8] =
                            unsafe { std::slice::from_raw_parts_mut(buf.0, block_size) };
                        let file = unsafe { std::fs::File::from_raw_fd(file_fd) };
                        file.read_at(buf, offset_in_file).unwrap();
                        file.into_raw_fd(); // so that it's there for next iteration
                    }
                    ClientWorkFd::TimerFd(timerfd, duration) => {
                        let mut fd = unsafe { timerfd::TimerFd::from_raw_fd(timerfd) };
                        fd.set_state(
                            timerfd::TimerState::Oneshot(duration),
                            timerfd::SetTimeFlags::Default,
                        );
                        fd.read();
                        let owned: OwnedFd = fd.into();
                        std::mem::forget(owned);
                    }
                    ClientWorkFd::NoWork => {}
                }
            })
            .await
            .unwrap();
            stats_state.reads_in_last_second[usize::try_from(i).unwrap()]
                .fetch_add(1, Ordering::Relaxed);
            if i == 0 {
                stats_state
                    .client0_latency_sample
                    .store(start.elapsed().as_micros() as u64, Ordering::Relaxed);
            }
        }
        info!("Client {i} stopping");
    }
}

struct EngineTokioFlume {
    rt: tokio::runtime::Runtime,
    num_workers: NonZeroU64,
    queue_depth: NonZeroU64,
}

impl Engine for EngineTokioFlume {
    fn run(
        self: Box<Self>,
        args: &'static Args,
        works: Vec<ClientWork>,
        stop: Arc<AtomicBool>,
        stats_state: Arc<StatsState>,
    ) {
        let myself = Arc::new(*self);
        let (worker_tx, work_rx) = flume::bounded(myself.queue_depth.get() as usize);
        let mut handles = Vec::new();
        for _ in 0..myself.num_workers.get() {
            let work_rx = work_rx.clone();
            let handle = std::thread::spawn(move || Self::worker(work_rx));
            handles.push(handle);
        }
        // workers get stopped by the worker_tx being dropped
        scopeguard::defer!(for handle in handles {
            handle.join().unwrap();
        });
        myself.rt.block_on(async {
            let mut handles = Vec::new();
            for (i, work) in (0..args.num_clients.get()).zip(works.into_iter()) {
                let stop = Arc::clone(&stop);
                let stats_state = Arc::clone(&stats_state);
                let worker_tx = worker_tx.clone();
                let myself = Arc::clone(&myself);
                handles.push(tokio::spawn(async move {
                    myself
                        .client(worker_tx, i, &args, work, &stop, stats_state)
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
        work: ClientWork,
        stop: &AtomicBool,
        stats_state: Arc<StatsState>,
    ) {
        tracing::info!("Client {i} starting");
        let block_size = 1 << args.block_size_shift.get();

        let rwlock = Arc::new(tokio::sync::RwLock::new(()));
        std::mem::forget(Arc::clone(&rwlock));

        #[derive(Copy, Clone)]
        enum ClientWorkFd {
            DiskAccess(RawFd),
            TimerFd(RawFd, Duration),
            NoWork,
        }

        let fd = match work {
            ClientWork::DiskAccess { file } => ClientWorkFd::DiskAccess(file.into_raw_fd()),
            ClientWork::TimerFdSetStateAndRead { timerfd, duration } => {
                let ret = ClientWorkFd::TimerFd(timerfd.as_raw_fd(), duration);
                std::mem::forget(timerfd); // they don't support into_raw_fd
                ret
            }
            ClientWork::NoWork {} => ClientWorkFd::NoWork,
        };

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
            // simulate Timeline::layers.read().await
            let _guard = rwlock.read().await;

            // find a random aligned 8k offset inside the file
            debug_assert!(1024 * 1024 % block_size == 0);
            let offset_in_file = rand::thread_rng()
                .gen_range(0..=((args.file_size_mib.get() * 1024 * 1024 - 1) / block_size_u64))
                * block_size_u64;
            let work = Box::new(move || {
                match fd {
                    ClientWorkFd::DiskAccess(file_fd) => {
                        let buf = buf;
                        let buf: &mut [u8] =
                            unsafe { std::slice::from_raw_parts_mut(buf.0, block_size) };
                        let file = unsafe { std::fs::File::from_raw_fd(file_fd) };
                        file.read_at(buf, offset_in_file).unwrap();
                        file.into_raw_fd(); // so that it's there for next iteration
                        Ok(())
                    }
                    ClientWorkFd::TimerFd(timerfd, duration) => {
                        let mut fd = unsafe { timerfd::TimerFd::from_raw_fd(timerfd) };
                        fd.set_state(
                            timerfd::TimerState::Oneshot(duration),
                            timerfd::SetTimeFlags::Default,
                        );
                        fd.read();
                        let owned: OwnedFd = fd.into();
                        std::mem::forget(owned);
                        Ok(())
                    }
                    ClientWorkFd::NoWork => Ok(()),
                }
            });
            // TODO: can this dealock with rendezvous channel, i.e., queue_depth=0?
            let (response_tx, response_rx) = tokio::sync::oneshot::channel();
            let start = std::time::Instant::now();
            worker_tx
                .send_async(FlumeWorkRequest {
                    work,
                    response: response_tx,
                })
                .await
                .unwrap();
            response_rx
                .await
                .expect("rx flume")
                .expect("not expecting io errors");
            stats_state.reads_in_last_second[usize::try_from(i).unwrap()]
                .fetch_add(1, Ordering::Relaxed);
            if i == 0 {
                stats_state
                    .client0_latency_sample
                    .store(start.elapsed().as_micros() as u64, Ordering::Relaxed);
            }
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

thread_local! {
    static RIO_THREAD_LOCAL_RING: std::cell::RefCell<(ThreadLocalRio, bool)>  = std::cell::RefCell::new((Arc::new(Mutex::new(Some(Arc::new(rio::new().unwrap())))), true));
    static RIO_EPOLL_THREAD_LOCAL_RING: std::cell::RefCell<(ThreadLocalRio, Option<rio::Reaper>)>  = std::cell::RefCell::new({
        let (rio, reaper) = rio::Config::default().start(true).unwrap();
        (Arc::new(Mutex::new(Some(Arc::new(rio)))), reaper)
    });
}

impl Engine for EngineTokioRio {
    fn run(
        self: Box<Self>,
        args: &'static Args,
        works: Vec<ClientWork>,
        stop: Arc<AtomicBool>,
        stats_state: Arc<StatsState>,
    ) {
        let EngineTokioRio { rt, mode } = *self;
        let rt = Arc::new(rt);

        let launch_eventfd_poller: LaunchEventfdPollerFn = Arc::new({
            let rt = Arc::clone(&rt);
            let stats_state = Arc::clone(&stats_state);
            move |rio: &rio::Rio, reaper: rio::Reaper| {
                let rt = Arc::clone(&rt);
                let stats_state = Arc::clone(&stats_state);
                let eventfd = eventfd::EventFD::new(0, eventfd::EfdFlags::EFD_NONBLOCK).unwrap();
                rio.register_eventfd_async(eventfd.as_raw_fd()).unwrap(); // TODO lifetime of the fd!
                let stats_state = Arc::clone(&stats_state);
                // XXX: figure out if unconstrainted is actually necessary, what we want is priority boost for this task; it's the most important one
                rt.spawn(tokio::task::unconstrained(async move {
                    let make_guard = |reaper: rio::Reaper| {
                        scopeguard::guard(reaper, |mut reaper| loop {
                            match reaper.block() {
                                std::ops::ControlFlow::Continue(_) => {
                                    continue;
                                }
                                std::ops::ControlFlow::Break(_) => {
                                    info!("Reaper task Drop observed poison");
                                    return;
                                }
                            }
                        })
                    };
                    let mut read_to_poison_on_drop = Some(make_guard(reaper));

                    let fd = tokio::io::unix::AsyncFd::new(eventfd).unwrap();
                    loop {
                        // info!("Reaper waiting for eventfd");
                        // See read() API docs for recipe for this code block.
                        loop {
                            let mut guard = fd.ready(tokio::io::Interest::READABLE).await.unwrap();
                            if !guard.ready().is_readable() {
                                info!("spurious wakeup");
                                continue;
                            }
                            match fd.get_ref().read() {
                                Ok(val) => {
                                    assert!(val > 0);
                                    // info!("read: {val:?}");
                                    continue;
                                }
                                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                                    // info!("would block");
                                    guard.clear_ready_matching(tokio::io::Ready::READABLE);
                                    break;
                                }
                                Err(e) => panic!("{:?}", e),
                            }
                        }

                        stats_state
                            .tokio_rio_epoll_iterations
                            .fetch_add(1, Ordering::Relaxed);

                        // info!("Reaper one iter");
                        let mut reaper = scopeguard::ScopeGuard::into_inner(
                            read_to_poison_on_drop.take().unwrap(),
                        );
                        match reaper.poll() {
                            std::ops::ControlFlow::Continue(_count) => {
                                // info!("Reaper poll got count {}", count);
                                read_to_poison_on_drop = Some(make_guard(reaper));
                                continue;
                            }
                            std::ops::ControlFlow::Break(()) => {
                                info!("Reaper stopping, poison pill");
                                break;
                            }
                        }
                    }
                }));
            }
        });

        let mut global_rios = vec![];
        let all_thread_local_rios = Arc::new(Mutex::new(Vec::new()));
        let add_to_thread_local_rios: TrackThreadLocalRioFn = {
            let rios = Arc::clone(&all_thread_local_rios);
            Arc::new(move |rio: &Arc<Mutex<Option<Arc<rio::Rio>>>>| {
                rios.lock().unwrap().push(Arc::clone(rio));
            })
        };

        let mode = match mode {
            TokioRioMode::SingleGlobal(rio) => {
                global_rios.push(Arc::clone(&rio));
                TokioRioModeReduced::SingleGlobal(rio)
            }
            TokioRioMode::ExecutorThreadLocal => TokioRioModeReduced::ExecutorThreadLocal {
                track_thread_local_rio: add_to_thread_local_rios,
            },
            TokioRioMode::Epoll { mut reaper, rio } => {
                let reaper = reaper.take().unwrap();
                launch_eventfd_poller(&rio, reaper);
                global_rios.push(Arc::clone(&rio));
                TokioRioModeReduced::Epoll(rio)
            }
            TokioRioMode::EpollExecutorThreadLocal => {
                TokioRioModeReduced::EpollExecutorThreadLocal {
                    track_thread_local_rio: Arc::new({
                        let add_to_thread_local_rios = Arc::clone(&add_to_thread_local_rios);
                        move |rio, reaper| {
                            add_to_thread_local_rios(rio);
                            let rio = {
                                let rio = rio.lock().unwrap();
                                let rio = rio.as_ref().unwrap();
                                Arc::clone(rio)
                            };
                            launch_eventfd_poller(&rio, reaper);
                        }
                    }),
                }
            }
        };
        rt.block_on(async move {
            let mut handles = Vec::new();
            for (i, work) in (0..args.num_clients.get()).zip(works.into_iter()) {
                let stop = Arc::clone(&stop);
                let stats_state = Arc::clone(&stats_state);
                let mode = mode.clone();
                handles.push(tokio::spawn(async move {
                    Self::client(mode, i, &args, work, &stop, stats_state).await
                }));
            }
            for handle in handles {
                handle.await.unwrap();
            }
        });
        // send the poison pill do make executors exit
        for rio in global_rios {
            let rio = Arc::try_unwrap(rio).expect("there should be no other owners left");
            drop(rio); // send the poison pill
        }
        for rio in all_thread_local_rios.lock().unwrap().iter() {
            let rio = rio.lock().unwrap().take().unwrap();
            drop(rio); // send the poison pill
        }
    }
}

impl EngineTokioRio {
    async fn client(
        mode: TokioRioModeReduced,
        i: u64,
        args: &Args,
        work: ClientWork,
        stop: &AtomicBool,
        stats_state: Arc<StatsState>,
    ) {
        // tokio::time::sleep(Duration::from_secs(i)).await;
        // tracing::info!("Client {i} starting");
        let block_size = 1 << args.block_size_shift.get();

        let rwlock = Arc::new(tokio::sync::RwLock::new(()));
        std::mem::forget(Arc::clone(&rwlock));

        #[derive(Copy, Clone)]
        enum ClientWorkFd {
            DiskAccess(RawFd),
            TimerFd(RawFd, Duration),
            NoWork,
        }

        let fd = match work {
            ClientWork::DiskAccess { file } => ClientWorkFd::DiskAccess(file.into_raw_fd()),
            ClientWork::TimerFdSetStateAndRead { timerfd, duration } => {
                let ret = ClientWorkFd::TimerFd(timerfd.as_raw_fd(), duration);
                std::mem::forget(timerfd); // they don't support into_raw_fd
                ret
            }
            ClientWork::NoWork {} => ClientWorkFd::NoWork,
        };

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
            // simulate Timeline::layers.read().await
            let _guard = rwlock.read().await;

            // find a random aligned 8k offset inside the file
            debug_assert!(1024 * 1024 % block_size == 0);
            let offset_in_file = rand::thread_rng()
                .gen_range(0..=((args.file_size_mib.get() * 1024 * 1024 - 1) / block_size_u64))
                * block_size_u64;

            let start = std::time::Instant::now();
            match fd {
                ClientWorkFd::DiskAccess(file_fd) => {
                    let buf = buf;
                    let mut buf: &mut [u8] =
                        unsafe { std::slice::from_raw_parts_mut(buf.0, block_size) };
                    let file = unsafe { std::fs::File::from_raw_fd(file_fd) };
                    // We use it to get one io_uring submission & completion ring per core / executor thread.
                    // The thread-local rings are not great if there's block_in_place in the codebase. It's fine here.
                    // Ideally we'd have one submission ring per core and a single completion ring, because, completion
                    // wakes up the task but we don't know which runtime it is one.
                    // (Even more ideal: a runtime that is io_uring-aware and keeps tasks that wait for wakeup from a completion
                    //  affine to a completion queue somehow... The design space is big.)
                    let rio = match &mode {
                        TokioRioModeReduced::SingleGlobal(rio) => rio.clone(),
                        TokioRioModeReduced::ExecutorThreadLocal {
                            track_thread_local_rio,
                        } => {
                            loop {
                                let got_it =
                                    RIO_THREAD_LOCAL_RING.with(|tl| match &mut *tl.borrow_mut() {
                                        (rio, needs_tracking @ true) => {
                                            track_thread_local_rio(rio);
                                            *needs_tracking = false;
                                            None
                                        }
                                        // fast path, will always be taken except fist time
                                        (rio, _needs_tracking @ false) => {
                                            let rio = rio.try_lock().unwrap();
                                            Some(Arc::clone(rio.as_ref().unwrap()))
                                        }
                                    });
                                if let Some(rio) = got_it {
                                    break rio;
                                }
                            }
                        }
                        TokioRioModeReduced::Epoll(rio) => rio.clone(),
                        TokioRioModeReduced::EpollExecutorThreadLocal {
                            track_thread_local_rio: setup_eventfd_poller,
                            ..
                        } => {
                            loop {
                                let got_it = RIO_EPOLL_THREAD_LOCAL_RING.with(|tl| match &mut *tl
                                    .borrow_mut()
                                {
                                    // initial path
                                    (rio, reaper @ Some(_)) => {
                                        let reaper = reaper.take().unwrap(); // side-effect: we'll take fast path next time
                                        setup_eventfd_poller(&rio, reaper);
                                        None
                                    }
                                    // fast path
                                    (rio, None) => {
                                        // already set up
                                        let rio = rio.try_lock().unwrap();
                                        Some(Arc::clone(rio.as_ref().unwrap()))
                                    }
                                });
                                if let Some(rio) = got_it {
                                    break rio;
                                }
                            }
                        }
                    };
                    let count = rio.read_at(&file, &mut buf, offset_in_file).await.unwrap();
                    assert_eq!(count, buf.len());
                    file.into_raw_fd(); // so that it's there for next iteration
                }
                ClientWorkFd::TimerFd(_timerfd, _duration) => {
                    unimplemented!()
                }
                ClientWorkFd::NoWork => (),
            }
            // TODO: can this dealock with rendezvous channel, i.e., queue_depth=0?

            stats_state.reads_in_last_second[usize::try_from(i).unwrap()]
                .fetch_add(1, Ordering::Relaxed);
            if i == 0 {
                stats_state
                    .client0_latency_sample
                    .store(start.elapsed().as_micros() as u64, Ordering::Relaxed);
            }
        }
        info!("Client {i} stopping");
    }
}

struct EngineTokioIoUringEventfdBridge {
    rt: tokio::runtime::Runtime,
}

impl Engine for EngineTokioIoUringEventfdBridge {
    fn run(
        self: Box<Self>,
        args: &'static Args,
        works: Vec<ClientWork>,
        stop: Arc<AtomicBool>,
        stats_state: Arc<StatsState>,
    ) {
        let EngineTokioIoUringEventfdBridge { rt } = *self;
        let rt = Arc::new(rt);

        rt.block_on(async move {
            let mut handles = Vec::new();
            for (i, work) in (0..args.num_clients.get()).zip(works.into_iter()) {
                let stop = Arc::clone(&stop);
                let stats_state = Arc::clone(&stats_state);
                handles.push(tokio::spawn(async move {
                    Self::client(i, &args, work, &stop, stats_state).await
                }));
            }
            // task that prints periodically which clients have exited
            let stopped_handles = Arc::new(
                (0..handles.len())
                    .map(|_| AtomicBool::new(false))
                    .collect::<Vec<_>>(),
            );
            let stop_stopped_task_status_task = Arc::new(AtomicBool::new(false));
            let stopped_task_status_task = tokio::spawn({
                let stopped_handles = Arc::clone(&stopped_handles);
                let stop_clients = Arc::clone(&stop);
                let stop_stopped_task_status_task = Arc::clone(&stop_stopped_task_status_task);
                async move {
                    // don't print until `stop` is set
                    while !stop_clients.load(Ordering::Relaxed) {
                        tokio::time::sleep(Duration::from_millis(1000)).await;
                        info!("waiting for clients to stop");
                    }
                    while !stop_stopped_task_status_task.load(Ordering::Relaxed) {
                        // log list of not-stopped clients every second
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        let stopped = stopped_handles
                            .iter()
                            .map(|x| x.load(Ordering::Relaxed))
                            .filter(|x| *x)
                            .count();
                        let not_stopped = stopped_handles
                            .iter()
                            .enumerate()
                            .filter(|(_, state)| state.load(Ordering::Relaxed) == false)
                            .map(|(i, _)| i)
                            .collect::<Vec<usize>>();
                        let total = stopped_handles.len();
                        info!("handles stopped {stopped} total {total}");
                        info!("  not stopped: {not_stopped:?}",)
                    }
                }
            });
            for (i, handle) in handles.into_iter().enumerate() {
                info!("awaiting client {i}");
                handle.await.unwrap();
                stopped_handles[i].store(true, Ordering::Relaxed);
            }
            stop_stopped_task_status_task.store(true, Ordering::Relaxed);
            info!("awaiting stopped_task_status_task");
            stopped_task_status_task.await.unwrap();
            info!("stopped_task_status_task stopped");
        });
    }
}

impl EngineTokioIoUringEventfdBridge {
    #[tracing::instrument(skip_all, level="trace", fields(client=%i))]
    async fn client(
        i: u64,
        args: &Args,
        work: ClientWork,
        stop: &AtomicBool,
        stats_state: Arc<StatsState>,
    ) {
        // tokio::time::sleep(Duration::from_secs(i)).await;
        // tracing::info!("Client {i} starting");
        let block_size = 1 << args.block_size_shift.get();

        let rwlock = Arc::new(tokio::sync::RwLock::new(()));
        std::mem::forget(Arc::clone(&rwlock));

        #[derive(Copy, Clone)]
        enum ClientWorkFd {
            DiskAccess(RawFd),
            TimerFd(RawFd, Duration),
            NoWork,
        }

        let fd = match work {
            ClientWork::DiskAccess { file } => ClientWorkFd::DiskAccess(file.into_raw_fd()),
            ClientWork::TimerFdSetStateAndRead { timerfd, duration } => {
                let ret = ClientWorkFd::TimerFd(timerfd.as_raw_fd(), duration);
                std::mem::forget(timerfd); // they don't support into_raw_fd
                ret
            }
            ClientWork::NoWork {} => ClientWorkFd::NoWork,
        };

        // alloc aligned to make O_DIRECT work
        let buf = unsafe {
            let ptr = std::alloc::alloc(Layout::from_size_align(block_size, block_size).unwrap());
            assert!(!ptr.is_null());
            Vec::from_raw_parts(ptr, 0, block_size)
        };
        let mut loop_buf = Some(buf);
        let block_size_u64: u64 = block_size.try_into().unwrap();
        while !stop.load(Ordering::Relaxed) {
            // simulate Timeline::layers.read().await
            let _guard = rwlock.read().await;

            // find a random aligned 8k offset inside the file
            debug_assert!(1024 * 1024 % block_size == 0);
            let offset_in_file = rand::thread_rng()
                .gen_range(0..=((args.file_size_mib.get() * 1024 * 1024 - 1) / block_size_u64))
                * block_size_u64;

            let start = std::time::Instant::now();
            match fd {
                ClientWorkFd::DiskAccess(file_fd) => {
                    let owned_buf = loop_buf.take().unwrap();
                    let file = unsafe { OwnedFd::from_raw_fd(file_fd) };
                    // We use it to get one io_uring submission & completion ring per core / executor thread.
                    // The thread-local rings are not great if there's block_in_place in the codebase. It's fine here.
                    // Ideally we'd have one submission ring per core and a single completion ring, because, completion
                    // wakes up the task but we don't know which runtime it is one.
                    // (Even more ideal: a runtime that is io_uring-aware and keeps tasks that wait for wakeup from a completion
                    //  affine to a completion queue somehow... The design space is big.)

                    let (file, owned_buf, res) =
                        tokio_io_uring_eventfd_bridge::preadv(file, offset_in_file, owned_buf)
                            .await;
                    let count = res.unwrap();
                    assert_eq!(count, owned_buf.len());
                    loop_buf = Some(owned_buf);
                    file.into_raw_fd(); // so that it's there for next iteration
                }
                ClientWorkFd::TimerFd(_timerfd, _duration) => {
                    unimplemented!()
                }
                ClientWorkFd::NoWork => (),
            }
            // TODO: can this dealock with rendezvous channel, i.e., queue_depth=0?

            stats_state.reads_in_last_second[usize::try_from(i).unwrap()]
                .fetch_add(1, Ordering::Relaxed);
            if i == 0 {
                stats_state
                    .client0_latency_sample
                    .store(start.elapsed().as_micros() as u64, Ordering::Relaxed);
            }
        }
        info!("Client {i} stopping");
    }
}
