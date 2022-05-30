#![deny(missing_docs)]
//! A key-value store system

#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;

mod backend;
mod client;
mod common;
mod error;
mod server;
mod thread_pool;

pub use backend::{Engine, KvSled, KvStore, KvsEngine};
pub use client::KvsClient;
pub use common::{Request, Response};
pub use error::{KvError, Result};
pub use server::KvsServer;
pub use thread_pool::{NaiveThreadPool,SharedQueueThreadPool, RayonThreadPool, ThreadPool};


/// Default Engine tag file
pub const ENGINE_TAG_FILE: &str = ".engine";