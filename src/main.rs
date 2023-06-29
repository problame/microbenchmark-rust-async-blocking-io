use std::{
    alloc::Layout,
    io::Write,
    num::NonZeroU64,
    os::unix::prelude::{FileExt, OpenOptionsExt},
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
};

use rand::{Rng, RngCore};
use structopt::StructOpt;
use tracing::{info, trace};

#[derive(structopt::StructOpt)]
struct Args {
    num_clients: NonZeroU64,
    file_size_mib: NonZeroU64,
    block_size_shift: NonZeroU64,
}

enum EngineKind {
    Std,
}

enum Engine {
    Std(EngineStd),
}

struct EngineStd {}

fn main() {
    std::env::set_var("RUST_LOG", "info");

    tracing_subscriber::fmt::init();

    let args: &'static Args = Box::leak(Box::new(Args::from_args()));

    let stop = Arc::new(AtomicBool::new(false));

    setup_files(&args);

    let engine = Arc::new(setup_engine(&args));

    let mut clients = Vec::new();

    let reads_in_last_second = Arc::new(AtomicU64::new(0));

    for i in 0..args.num_clients.get() {
        let stop = Arc::clone(&stop);
        let engine = Arc::clone(&engine);
        let reads_in_last_second = Arc::clone(&reads_in_last_second);
        let jh = std::thread::spawn(move || {
            tracing::info!("Client {i} starting");
            let mut file = std::fs::OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_DIRECT)
                .open(data_file_path(args, i))
                .unwrap();
            let block_size = 1 << args.block_size_shift.get();
            // alloc aligned to make O_DIRECT work
            let buf = unsafe {
                std::alloc::alloc(Layout::from_size_align(block_size, block_size).unwrap())
            };
            assert!(!buf.is_null());
            let buf: &mut [u8] = unsafe { std::slice::from_raw_parts_mut(buf, block_size) };
            let block_size: u64 = block_size.try_into().unwrap();
            while !stop.load(Ordering::Relaxed) {
                // find a random aligned 8k offset inside the file
                debug_assert!(1024 * 1024 % block_size == 0);
                let offset_in_file = rand::thread_rng()
                    .gen_range(0..=((args.file_size_mib.get() * 1024 * 1024 - 1) / block_size))
                    * block_size;
                // call the engine to make the io
                match &*engine {
                    Engine::Std(engine) => {
                        engine.read_iter(i, args, &mut file, offset_in_file, buf)
                    }
                }
                reads_in_last_second.fetch_add(1, Ordering::Relaxed);
            }
            info!("Client {i} stopping");
        });
        clients.push(jh);
    }

    ctrlc::set_handler({
        let stop = Arc::clone(&stop);
        move || {
            stop.store(true, Ordering::Relaxed);
        }
    });

    std::thread::spawn(move || {
        while !stop.load(Ordering::Relaxed) {
            std::thread::sleep(std::time::Duration::from_secs(1));
            let reads_in_last_second = reads_in_last_second.swap(0, Ordering::Relaxed);
            info!(
                "IOPS {}k BANDWIDTH {} MiB/s",
                reads_in_last_second,
                (1 << args.block_size_shift.get()) * reads_in_last_second / (1 << 20),
            );
        }
    })
    .join();

    for jh in clients {
        jh.join().unwrap();
    }
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

fn setup_engine(args: &Args) -> Engine {
    Engine::Std(EngineStd {})
}

impl EngineStd {
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
