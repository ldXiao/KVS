// #![deny(missing_docs)]
//! A key-value store system

#![allow(dead_code)]
// #![allow(unused)]

#[macro_use]
extern crate log;

mod engine;
mod client;
mod config;
mod error;
mod percolator;
mod raft;
mod rpc;
mod server;

/// Thread Pool
pub mod thread_pool;

pub use engine::{EngineKind, KvSled, KvStore, KvsEngine};
pub use client::{KvsClient, KvsClientBuilder};
pub use error::{KvError, KvRpcError, Result};
pub use raft::{FilePersister, KvRaftNode, Persister, RaftNode};
pub use percolator::{DataValue, Key, LockValue, MultiStore, TimestampDump, WriteValue};
pub use server::{KvsServer, KvsServerBuilder};

/// preclude
pub mod preclude {
    pub use crate::engine::{EngineKind, KvSled, KvStore, KvsEngine};
    pub use crate::client::{KvsClient, KvsClientBuilder};
    pub use crate::error::{KvError, Result};
    pub use crate::percolator::{
        DataValue, Key, LockValue, MultiStore, TimestampDump, WriteValue,
    };
    pub use crate::raft::{FilePersister, KvRaftNode, Persister, RaftNode};
    #[allow(missing_docs)]
    pub use crate::rpc::kvs_service::*;
    #[allow(missing_docs)]
    pub use crate::rpc::raft_service::*;
    pub use crate::server::{KvsServer, KvsServerBuilder};
}
