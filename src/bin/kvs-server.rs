#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;

use kvs::thread_pool::*;
use kvs::*;
// use serde::{Deserialize, Serialize};
use slog::Drain;
use std::{env::current_dir, fs, io::Write, net::SocketAddr, str::FromStr};
use structopt::StructOpt;

const DEFAULT_ADDR: &str = "127.0.0.1:4000";

#[derive(Debug, StructOpt)]
#[structopt(
    name = env!("CARGO_PKG_NAME"), 
    about = env!("CARGO_PKG_DESCRIPTION"), 
    version = env!("CARGO_PKG_VERSION"), 
    author = env!("CARGO_PKG_AUTHORS")
)]
struct Opt {
    #[structopt(
        name = "IP-PORT",
        short = "a",
        long = "addr",
        default_value = DEFAULT_ADDR
    )]
    addr: SocketAddr,
    #[structopt(
        name = "ENGINE-NAME",
        short = "e",
        long = "engine",
        default_value = "Auto",
        parse(try_from_str = parse_str_to_engine)
    )]
    engine: Engine,
}

fn parse_str_to_engine(src: &str) -> Result<Engine> {
    let previous = fs::read_to_string(ENGINE_TAG_FILE);
    if src == "Auto" {
        Ok(Engine::from_str(&previous.unwrap_or("kvs".to_string()))?)
    } else if previous.is_err() || src == previous.unwrap() {
        Ok(Engine::from_str(src)?)
    } else {
        Err(KvError::ParserError(src.to_string()))
    }
}

fn write_engine_to_dir(engine: &Engine) -> Result<()> {
    let mut engine_tag_file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(ENGINE_TAG_FILE)?;
    match engine {
        Engine::kvs => engine_tag_file.write(b"kvs")?,
        Engine::sled => engine_tag_file.write(b"sled")?,
    };
    engine_tag_file.flush()?;
    Ok(())
}

fn main() -> Result<()> {
    // let decorator = slog_term::TermDecorator::new().build();
    let decorator = slog_term::PlainSyncDecorator::new(std::io::stderr());
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let logger = slog::Logger::root(drain, o!());

    let opt: Opt = Opt::from_args();
    write_engine_to_dir(&opt.engine)?;

    info!(logger, "Key Value Store Server");
    info!(logger, "Version : {}", env!("CARGO_PKG_VERSION"));
    info!(logger, "IP-PORT : {}", opt.addr);
    info!(logger, "Engine  : {:?}", opt.engine);

    let cpus = num_cpus::get();
    let thread_pool = SharedQueueThreadPool::new(cpus as u32)?;

    match opt.engine {
        Engine::kvs => {
            let mut server =
                KvsServer::new(KvStore::open(current_dir().unwrap())?, thread_pool, logger)?;
            server.run(opt.addr)?;
        }
        Engine::sled => {
            let mut server =
                KvsServer::new(KvSled::open(current_dir().unwrap())?, thread_pool, logger)?;
            server.run(opt.addr)?;
        }
    }

    Ok(())
}
