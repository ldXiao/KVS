use std::{
	cmp::{max, min},
	sync::Arc,
	sync::{
			atomic::{AtomicBool, AtomicU64, Ordering},
			Mutex,
	},
	thread,
};

use futures::FutureExt;
use prost::Message;
use tokio::{
	runtime::Builder,
	sync::{
			mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
			oneshot::{channel, Receiver, Sender},
	},
};
use tokio_stream::{Stream, StreamExt};
use tonic::{transport::Channel, Code, Request, Response, Status};


use super::persister::*;
use super::raft::{Ra};
use crate::preclude::*;
// use crate::rpc::raft_service::*;

/// RaftNode can replicate log entries to all raft nodes
#[derive(Clone)]
pub struct RaftNode {
    // Your code here.
    handle: Arc<Mutex<thread::JoinHandle<()>>>,
    me: usize,
    sender: UnboundedSender<RaftEvent>,
    term: Arc<AtomicU64>,
    is_leader: Arc<AtomicBool>,
    raft_state_size: Arc<AtomicU64>,
    commit_index: Arc<AtomicU64>,
    last_applied: Arc<AtomicU64>,
}

impl RaftNode {
    /// Create a new raft service.
    pub fn new(
        peers: Vec<RaftRpcClient<Channel>>,
        me: usize,
        persister: Arc<dyn Persister>,
        apply_ch: UnboundedSender<ApplyMsg>,
    ) -> RaftNode {
        let (mut raft, sender) = RaftCore::new(peers, me, persister, apply_ch);
        let term = raft.current_term.clone();
        let is_leader = raft.is_leader.clone();
        let raft_state_size = raft.raft_state_size.clone();
        let commit_index = raft.commit_index.clone();
        let last_applied = raft.last_applied.clone();

        let threaded_rt = Builder::new_multi_thread().enable_all().build().unwrap();
        let handle = thread::Builder::new()
            .name(format!("RaftNode-{}", me))
            .spawn(move || {
                threaded_rt.block_on(async move {
                    debug!("Enter main executor!");
                    while raft.next().await.is_some() {
                        trace!("get event");
                    }
                    debug!("Leave main executor!");
                })
            })
            .unwrap();
        RaftNode {
            handle: Arc::new(Mutex::new(handle)),
            me,
            sender,
            term,
            is_leader,
            raft_state_size,
            commit_index,
            last_applied,
        }
    }

    /// start a command that can encode to Bytes
    pub fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: Message,
    {
        if self.is_leader() {
            let mut buf = vec![];
            // labcodec::encode(command, &mut buf).unwrap();
            command.encode(&mut buf).unwrap();

            let threaded_rt = Builder::new_multi_thread().build().unwrap();

            let (tx, rx) = channel();
            let sender = self.sender.clone();
            let handle = thread::spawn(move || {
                sender
                    .send(RaftEvent::StartCommand(buf, tx))
                    .expect("Unable to send start command to RaftExecutor");

                let fut_values = async { rx.await };
                threaded_rt.block_on(fut_values).unwrap()
            });
            let response = handle.join().unwrap();
            debug!(
                "RaftNode {} -- Start a Command, response with: {:?}",
                self.me, response
            );
            response
        } else {
            debug!("RaftNode {} -- Start a Command but in Not Leader", self.me);
            Err(KvError::RaftErrorNotLeader)
        }
    }

    /// start a command that can encode to Bytes
    pub fn start_read_only<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: Message,
    {
        if self.is_leader() {
            let mut buf = vec![];
            // labcodec::encode(command, &mut buf).unwrap();
            command.encode(&mut buf).unwrap();

            let threaded_rt = Builder::new_multi_thread().build().unwrap();

            let (tx, rx) = channel();
            let sender = self.sender.clone();
            let handle = thread::spawn(move || {
                sender
                    .send(RaftEvent::StartReadOnly(buf, tx))
                    .expect("Unable to send start ReadOnly to RaftExecutor");

                let fut_values = async { rx.await };
                threaded_rt.block_on(fut_values).unwrap()
            });
            let response = handle.join().unwrap();
            debug!(
                "RaftNode {} -- Start a ReadOnly, response with: {:?}",
                self.me, response
            );
            response
        } else {
            debug!("RaftNode {} -- Start a ReadOnly but in Not Leader", self.me);
            Err(KvError::RaftErrorNotLeader)
        }
    }

    /// receive a snapshot and save it.
    pub fn start_snapshot(&self, snapshot: Vec<u8>, last_applied: u64) {
        self.sender
            .send(RaftEvent::StartSnapshot(snapshot, last_applied))
            .expect("Unable to send start Snapshot to RaftExecutor");

        debug!("RaftNode {} -- Start a Snapshot", self.me,);
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.term.load(Ordering::SeqCst)
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.is_leader.load(Ordering::SeqCst)
    }

    /// Shutdown this node
    pub fn kill(&self) {
        let _ = self.sender.send(RaftEvent::Shutdown);
    }
}

#[tonic::async_trait]
impl RaftRpc for RaftNode {
    async fn request_vote(
        &self,
        args: Request<RequestVoteArgs>,
    ) -> std::result::Result<Response<RequestVoteReply>, Status> {
        let (tx, rx) = channel();
        let event = RaftEvent::RequestVote(args.into_inner(), tx);
        let _ = self.sender.clone().send(event);
        let reply = rx.await;
        reply
            .map(|reply| Response::new(reply))
            .map_err(|e| Status::new(Code::Cancelled, e.to_string()))
    }

    async fn append_entries(
        &self,
        args: Request<AppendEntriesArgs>,
    ) -> std::result::Result<Response<AppendEntriesReply>, Status> {
        let (tx, rx) = channel();
        let event = RaftEvent::AppendEntries(args.into_inner(), tx);
        let _ = self.sender.clone().send(event);
        let reply = rx.await;
        reply
            .map(|reply| Response::new(reply))
            .map_err(|e| Status::new(Code::Cancelled, e.to_string()))
    }

    async fn heart_beat(
        &self,
        args: Request<HeartBeatArgs>,
    ) -> std::result::Result<Response<HeartBeatReply>, Status> {
        let (tx, rx) = channel();
        let event = RaftEvent::HeartBeat(args.into_inner(), tx);
        let _ = self.sender.clone().send(event);
        let reply = rx.await;
        reply
            .map(|reply| Response::new(reply))
            .map_err(|e| Status::new(Code::Cancelled, e.to_string()))
    }

    async fn install_snapshot(
        &self,
        args: Request<InstallSnapshotArgs>,
    ) -> std::result::Result<Response<InstallSnapshotReply>, Status> {
        let (tx, rx) = channel();
        let event = RaftEvent::InstallSnapshot(args.into_inner(), tx);
        let _ = self.sender.clone().send(event);
        let reply = rx.await;
        reply
            .map(|reply| Response::new(reply))
            .map_err(|e| Status::new(Code::Cancelled, e.to_string()))
    }
}
