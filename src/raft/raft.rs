use std::{
    cmp::{max, min},
    fmt::Display,
    sync::Arc,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Mutex,
    },
    task::Poll,
    thread,
    time::Duration,
};

use futures::FutureExt;
use futures_timer::Delay;
use prost::Message;
use rand::Rng;
use tokio::{
    runtime::Builder,
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        oneshot::{channel, Receiver, Sender},
    },
};
use tokio_stream::{Stream, StreamExt};
use tonic::{transport::Channel, Code, Request, Response, Status};

fn election_timeout() -> Duration {
    let variant = rand::thread_rng().gen_range(600, 800);
    Duration::from_millis(variant)
}

fn heartbeat_timeout() -> Duration {
    // let variant = rand::thread_rng().gen_range(100, 104);
    let variant = 50;
    Duration::from_millis(variant)
}

use super::persister::*;
use crate::preclude::*;
// use crate::rpc::raft_service::*;

#[derive(Message, Clone)]
pub struct ApplyMsg {
    #[prost(bool, tag = "1")]
    pub command_valid: bool,
    #[prost(bytes, tag = "2")]
    pub command: Vec<u8>,
    #[prost(uint64, tag = "3")]
    pub command_index: u64,
}

#[derive(Message, Clone)]
pub struct Persistent {
    #[prost(uint64, tag = "1")]
    pub current_term: u64,
    #[prost(int32, tag = "2")]
    pub voted_for: i32,
    #[prost(message, repeated, tag = "3")]
    pub log: Vec<LogEntry>,
    #[prost(uint64, tag = "4")]
    pub last_included_index: u64,
    #[prost(uint64, tag = "5")]
    pub last_included_term: u64,
}

#[derive(PartialEq, Clone)]
enum RaftRole {
    Follower,
    Candidate,
    Leader,
}

// A single ReftCore peer.
struct RaftCore {
    // RPC end points of all peers
    peers: Vec<RaftRpcClient<Channel>>,
    // Object to hold this peer's persisted state
    persister: Arc<dyn Persister>,
    // this peer's index into peers[]
    id: usize,

    // Persistent state on all servers
    // Updated on stable storage before responding to RPCs
    current_term: Arc<AtomicU64>,
    voted_for: Option<usize>,
    log: Vec<LogEntry>,

    // auxilary state
    role: RaftRole,
    is_leader: Arc<AtomicBool>,

    // Volatile state on all servers
    commit_index: Arc<AtomicU64>,
    last_applied: Arc<AtomicU64>,

    // Volatile state on leader
    // Reinitialized after election
    next_index: Vec<Arc<AtomicU64>>,
    match_index: Vec<Arc<AtomicU64>>,

    // Persistent state on all servers
    // Add when using Snapshot
    last_included_index: Arc<AtomicU64>,
    last_included_term: Arc<AtomicU64>,

    // current save size
    // update when persist
    raft_state_size: Arc<AtomicU64>,

    // ApplyMsg channel
    sender: UnboundedSender<RaftEvent>,
    apply_ch: UnboundedSender<ApplyMsg>,

    // Read Only
    read_only: super::read_only::ReadOnly,

    // for stream
    receiver: UnboundedReceiver<RaftEvent>,
    timeout: Delay,
    apply_msg_delay: Delay,
}

impl Display for RaftCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let role = match self.role {
            RaftRole::Follower => "Follower ",
            RaftRole::Candidate => "Candidate",
            RaftRole::Leader => "Leader   ",
        };
        write!(
            f,
            "[{} {}] [Term {}] [Snap {} {}] [Log {} {}] [Commit {} {}]",
            role,
            self.id,
            self.current_term.load(Ordering::SeqCst),
            self.last_included_index.load(Ordering::SeqCst),
            self.last_included_term.load(Ordering::SeqCst),
            self.log.len(),
            self.log.last().map_or(0, |v| v.term),
            self.commit_index.load(Ordering::SeqCst),
            self.last_applied.load(Ordering::SeqCst),
        )
    }
}

impl RaftCore {
    /// the service or tester wants to create a ReftCore server. the ports
    /// of all the ReftCore servers (including this one) are in peers. this
    /// server's port is peers[me]. all the servers' peers arrays
    /// have the same order. persister is a place for this server to
    /// save its persistent state, and also initially holds the most
    /// recent saved state, if any. apply_ch is a channel on which the
    /// tester or service expects ReftCore to send ApplyMsg messages.
    /// This method must return quickly.
    pub fn new(
        peers: Vec<RaftRpcClient<Channel>>,
        me: usize,
        persister: Arc<dyn Persister>,
        apply_ch: UnboundedSender<ApplyMsg>,
    ) -> (RaftCore, UnboundedSender<RaftEvent>) {
        let raft_state = persister.raft_state();
        let peers_num = peers.len();

        let (sender, receiver) = unbounded_channel();

        let mut rf = RaftCore {
            peers,
            persister,
            id: me,
            // state: Arc::default(),
            current_term: Arc::new(AtomicU64::new(0)),
            voted_for: None,
            log: vec![],

            is_leader: Arc::new(AtomicBool::new(false)),
            role: RaftRole::Follower,

            commit_index: Arc::new(AtomicU64::new(0)),
            last_applied: Arc::new(AtomicU64::new(0)),

            last_included_index: Arc::new(AtomicU64::new(0)),
            last_included_term: Arc::new(AtomicU64::new(0)),
            raft_state_size: Arc::new(AtomicU64::new(0)),

            next_index: Vec::new(),
            match_index: Vec::new(),

            sender: sender.clone(),
            apply_ch,

            read_only: super::read_only::ReadOnly::new(),

            // for stream
            receiver,
            timeout: Delay::new(election_timeout()),
            apply_msg_delay: Delay::new(heartbeat_timeout()),
        };

        for _i in 0..peers_num {
            rf.next_index.push(Arc::new(AtomicU64::new(0)));
            rf.match_index.push(Arc::new(AtomicU64::new(0)));
        }

        // initialize from state persisted before a crash
        rf.restore(&raft_state);

        debug!("{} Started!", rf);

        (rf, sender)
    }

    /// save ReftCore's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&mut self) {
        let mut data = Vec::new();
        let per = Persistent {
            current_term: self.current_term.load(Ordering::SeqCst),
            voted_for: self.voted_for.map_or(-1, |v| v as i32),
            log: self.log.clone(),
            last_included_index: self.last_included_index.load(Ordering::SeqCst),
            last_included_term: self.last_included_term.load(Ordering::SeqCst),
        };
        per.encode(&mut data).unwrap();
        self.raft_state_size
            .store(data.len() as u64, Ordering::SeqCst);
        self.persister.save_raft_state(data);
    }

    /// save ReftCore's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist_with_snapshot(&mut self, snapshot: Vec<u8>) {
        let mut data = Vec::new();
        let per = Persistent {
            current_term: self.current_term.load(Ordering::SeqCst),
            voted_for: self.voted_for.map_or(-1, |v| v as i32),
            log: self.log.clone(),
            last_included_index: self.last_included_index.load(Ordering::SeqCst),
            last_included_term: self.last_included_term.load(Ordering::SeqCst),
        };
        per.encode(&mut data).unwrap();
        self.persister.save_state_and_snapshot(data, snapshot);
    }

    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any state?
            return;
        }
        // match labcodec::decode::<Persistent>(data) {
        match Persistent::decode(data) {
            Ok(o) => {
                self.current_term = Arc::new(AtomicU64::new(o.current_term));
                self.voted_for = {
                    match o.voted_for < 0 {
                        true => Some(o.voted_for as usize),
                        false => None,
                    }
                };
                self.log = o.log;
                self.last_included_index = Arc::new(AtomicU64::new(o.last_included_index));
                self.last_included_term = Arc::new(AtomicU64::new(o.last_included_term));
                self.commit_index = Arc::new(AtomicU64::new(o.last_included_index));
                self.last_applied = Arc::new(AtomicU64::new(o.last_included_index));
            }
            Err(e) => {
                panic!("{:?}", e);
            }
        }
    }

    fn send_apply_msg(&mut self) {
        while !self.apply_ch.is_closed()
            && self.last_applied.load(Ordering::SeqCst) < self.commit_index.load(Ordering::SeqCst)
        {
            let index = (self.last_applied.load(Ordering::SeqCst)
                - self.last_included_index.load(Ordering::SeqCst)) as usize;
            // let mut apply_ch = self.apply_ch.clone();
            let msg = ApplyMsg {
                command_valid: true,
                command: self.log[index].command.to_owned(),
                command_index: self.last_applied.load(Ordering::SeqCst) + 1,
            };
            self.apply_ch.send(msg).expect("Unable send ApplyMsg");
            self.last_applied.fetch_add(1, Ordering::SeqCst);
            if self.is_leader.load(Ordering::SeqCst) {
                info!(
                    "{} Apply command: [ApplyMsg {} Term {}]",
                    self,
                    self.last_applied.load(Ordering::SeqCst),
                    self.log[index].term
                );
            }
        }
    }

    fn start(&mut self, command: &[u8]) -> Result<(u64, u64)> {
        let index = self.log.len() as u64 + self.last_included_index.load(Ordering::SeqCst) + 1;
        let term = self.current_term.load(Ordering::SeqCst);
        let is_leader = self.is_leader.load(Ordering::SeqCst);

        if is_leader {
            self.log.push(LogEntry {
                command: command.to_owned(),
                index,
                term,
            });
            info!(
                "{} Receive a Command! Append to [log {} {}]",
                self, index, term
            );
            self.match_index[self.id] = Arc::new(AtomicU64::new(index as u64));
            self.persist();
            Ok((index, term))
        } else {
            Err(KvError::RaftErrorNotLeader)
        }
    }
    fn start_read_only(&mut self, command: &[u8]) -> Result<(u64, u64)> {
        let commit_index = self.commit_index.load(Ordering::SeqCst);
        if self.is_leader.load(Ordering::SeqCst) {
            if self.term(commit_index).unwrap() == self.current_term.load(Ordering::SeqCst) {
                self.read_only.add_request(commit_index, command.to_owned());
                self.send_heart_beat_all();
                Ok((commit_index, self.last_term()))
            } else {
                Err(KvError::StringError(
                    "New Leader without commit".to_string(),
                ))
            }
        } else {
            Err(KvError::RaftErrorNotLeader)
        }
    }

    fn apply_read_only(&mut self, index: u64, msgs: Vec<Vec<u8>>) {
        if !self.apply_ch.is_closed() {
            for msg in msgs {
                let msg = ApplyMsg {
                    command_valid: true,
                    command: msg,
                    command_index: index,
                };
                self.apply_ch.send(msg).expect("Unable send ApplyMsg");
            }
        }
    }

    fn get_last_log_info(&self) -> (u64, u64) {
        let mut index = self.last_included_index.load(Ordering::SeqCst);
        let mut term = self.last_included_term.load(Ordering::SeqCst);
        index += self.log.len() as u64;
        if !self.log.is_empty() {
            term = self.log.last().unwrap().term;
        }
        (index, term)
    }

    fn become_leader(&mut self, term: u64) {
        self.current_term.store(term, Ordering::SeqCst);
        self.role = RaftRole::Leader;
        self.is_leader.store(true, Ordering::SeqCst);
        let (index, _term) = self.get_last_log_info();
        for i in 0..self.peers.len() {
            self.next_index[i] = Arc::new(AtomicU64::new(index + 1));
            self.match_index[i] = Arc::new(AtomicU64::new(0));
        }
        self.match_index[self.id] = Arc::new(AtomicU64::new(index));
        self.persist();

        let last_included_index = self.last_included_index.load(Ordering::SeqCst);
        self.commit_index
            .store(last_included_index, Ordering::SeqCst);
        self.last_applied
            .store(last_included_index, Ordering::SeqCst);
        let snapshot = self.persister.snapshot();
        let msg = ApplyMsg {
            command_valid: false,
            command: snapshot,
            command_index: 0,
        };
        self.apply_ch.send(msg).expect("Unable send ApplyMsg");

        info!("{} Become Leader", self);
    }

    fn become_follower(&mut self, term: u64) {
        self.current_term.store(term, Ordering::SeqCst);
        // self.voted_for = None;
        self.role = RaftRole::Follower;
        self.is_leader.store(false, Ordering::SeqCst);
        self.persist();
        debug!("{} Become Follower", self);
    }

    fn become_candidate(&mut self) {
        self.current_term.fetch_add(1, Ordering::SeqCst);
        self.role = RaftRole::Candidate;
        self.is_leader.store(false, Ordering::SeqCst);
        self.voted_for = Some(self.id);
        self.persist();
        debug!("{} Become Candidate", self);

        self.send_request_vote_all();
    }

    fn update_commit_index(&mut self) {
        let last_included_index = self.last_included_index.load(Ordering::SeqCst);
        let last_included_term = self.last_included_term.load(Ordering::SeqCst);
        self.match_index[self.id].store(
            self.log.len() as u64 + last_included_index,
            Ordering::SeqCst,
        );
        let mut match_index_all: Vec<u64> = self
            .match_index
            .iter()
            .map(|v| v.load(Ordering::SeqCst))
            .collect();
        match_index_all.sort_unstable();
        let match_n = match_index_all[self.peers.len() / 2];
        if match_n > self.commit_index.load(Ordering::SeqCst)
            && (match_n == last_included_index
                || self
                    .log
                    .get((match_n - last_included_index - 1) as usize)
                    .map_or(last_included_term, |v| v.term)
                    == self.current_term.load(Ordering::SeqCst))
        {
            debug!("{} Update commit index: {}", self, match_n);
            self.commit_index.store(match_n, Ordering::SeqCst);
        }
        self.send_apply_msg();
    }
}

impl RaftCore {
    fn last_index(&self) -> u64 {
        self.log
            .last()
            .map_or(self.last_included_index.load(Ordering::SeqCst), |v| v.index)
    }
    fn last_term(&self) -> u64 {
        self.log
            .last()
            .map_or(self.last_included_term.load(Ordering::SeqCst), |v| v.term)
    }
    fn term(&self, idx: u64) -> Option<u64> {
        let last_included_index = self.last_included_index.load(Ordering::SeqCst);
        if idx < last_included_index {
            None
        } else if idx == last_included_index {
            Some(self.last_included_term.load(Ordering::SeqCst))
        } else if idx <= self.last_index() {
            Some(self.log[(idx - last_included_index - 1) as usize].term)
        } else {
            None
        }
    }
    fn match_term(&self, idx: u64, term: u64) -> bool {
        self.term(idx).map(|t| t == term).unwrap_or(false)
    }
    fn is_up_to_date(&self, last_index: u64, term: u64) -> bool {
        term > self.last_term() || (term == self.last_term() && last_index >= self.last_index())
    }
}

impl RaftCore {
    fn send_request_vote(
        &self,
        server: usize,
        args: RequestVoteArgs,
    ) -> Receiver<Result<RequestVoteReply>> {
        let peer = &self.peers[server];
        let mut peer_clone = peer.clone();
        let (tx, rx) = channel::<Result<RequestVoteReply>>();
        tokio::spawn(async move {
            let res = peer_clone
                .request_vote(Request::new(args))
                .await
                .map(|resp| resp.into_inner())
                .map_err(KvError::Rpc);
            let _ = tx.send(res);
        });
        rx
    }

    fn handle_request_vote(&mut self, args: RequestVoteArgs) -> RequestVoteReply {
        let current_term = self.current_term.load(Ordering::SeqCst);
        if current_term < args.term {
            self.voted_for = None;
            self.become_follower(args.term);
        }

        if args.term < current_term {
            debug!("{} Handle {}, Vote false due to older term", self, args);
            RequestVoteReply {
                term: current_term,
                vote_granted: false,
            }
        } else if self.voted_for.is_some() && self.voted_for != Some(args.candidate_id as usize) {
            debug!(
                "{} Handle {}, Vote false due to already vote for {}",
                self,
                args,
                self.voted_for.unwrap()
            );
            RequestVoteReply {
                term: args.term,
                vote_granted: false,
            }
        } else if !self.is_up_to_date(args.last_log_index, args.last_log_term) {
            debug!("{} Handle {}, Vote false due to older log", self, args);
            RequestVoteReply {
                term: args.term,
                vote_granted: false,
            }
        } else {
            info!("{} Handle {}, Vote true", self, args);
            self.voted_for = Some(args.candidate_id as usize);
            RequestVoteReply {
                term: args.term,
                vote_granted: true,
            }
        }
    }

    fn send_request_vote_all(&mut self) {
        let vote_count = Arc::new(AtomicUsize::new(1));
        let (last_log_index, last_log_term) = self.get_last_log_info();
        let args = RequestVoteArgs {
            term: self.current_term.load(Ordering::SeqCst),
            candidate_id: self.id as i32,
            last_log_index,
            last_log_term,
        };
        // let mut rx_vec = FuturesUnordered::new();
        info!("{} Send {} to ALL RaftNode", self, args);
        let is_candidate = Arc::new(AtomicBool::new(true));
        for server in 0..self.peers.len() {
            if server != self.id {
                let args = args.clone();
                let tx = self.sender.clone();
                let peers_num = self.peers.len();
                let is_candidate = is_candidate.clone();
                let term = self.current_term.load(Ordering::SeqCst);
                let vote_count = vote_count.clone();
                // rx_vec.push(self.send_request_vote(server, args));
                let rx = self.send_request_vote(server, args);
                tokio::spawn(async move {
                    if let Ok(reply) = rx.await {
                        if let Ok(reply) = reply {
                            if is_candidate.load(Ordering::SeqCst) {
                                debug!(
                                    "Get one {}, current {}, total {}",
                                    reply,
                                    vote_count.load(Ordering::SeqCst),
                                    peers_num
                                );
                                if reply.term > term {
                                    tx.send(RaftEvent::BecomeFollower(reply.term)).unwrap();
                                } else if reply.vote_granted {
                                    vote_count.fetch_add(1, Ordering::Relaxed);
                                    if vote_count.load(Ordering::SeqCst) > peers_num / 2 {
                                        is_candidate.store(false, Ordering::SeqCst);
                                        tx.send(RaftEvent::BecomeLeader(reply.term)).unwrap();
                                    }
                                }
                            }
                        }
                    }
                });
            }
        }
    }
}

impl RaftCore {
    fn send_append_entries(
        &self,
        server: usize,
        args: AppendEntriesArgs,
    ) -> Receiver<Result<AppendEntriesReply>> {
        let peer = &self.peers[server];
        let mut peer_clone = peer.clone();
        let (tx, rx) = channel::<Result<AppendEntriesReply>>();
        tokio::spawn(async move {
            let res = peer_clone
                .append_entries(Request::new(args))
                .await
                .map(|resp| resp.into_inner())
                .map_err(KvError::Rpc);
            let _ = tx.send(res);
        });
        rx
    }

    fn handle_append_entries(&mut self, args: AppendEntriesArgs) -> AppendEntriesReply {
        let last_included_index = self.last_included_index.load(Ordering::SeqCst);
        let last_included_term = self.last_included_term.load(Ordering::SeqCst);

        let current_term = self.current_term.load(Ordering::SeqCst);

        if current_term < args.term {
            self.voted_for = Some(args.leader_id as usize);
            self.become_follower(args.term);
            debug!(
                "{} Become Follower. New Leader id: {}",
                self, args.leader_id
            );
        }
        if args.term < current_term {
            debug!("{} Handle {}, Success false due to older term", self, args);
            AppendEntriesReply {
                term: current_term,
                success: false,
                conflict_log_index: 0,
                conflict_log_term: 0,
            }
        } else if args.prev_log_index < last_included_index
            || args.prev_log_index == last_included_index
                && args.prev_log_term != last_included_term
        {
            debug!(
                "{} Handle {}, Success false due to Snapshot not match",
                self, args
            );
            AppendEntriesReply {
                term: current_term,
                success: false,
                conflict_log_term: last_included_term,
                conflict_log_index: last_included_index,
            }
        } else if args.prev_log_index > self.last_index()
            || args.prev_log_index > last_included_index
                && !self.match_term(args.prev_log_index, args.prev_log_term)
        {
            debug!(
                "{} Handle {}, Success false due to log not match",
                self, args
            );
            let conflict_log_term = self.term(args.prev_log_index).unwrap_or(last_included_term);
            AppendEntriesReply {
                term: self.current_term.load(Ordering::SeqCst),
                success: false,
                conflict_log_term,
                conflict_log_index: self
                    .log
                    .iter()
                    .filter(|v| v.term == conflict_log_term)
                    .take(1)
                    .next()
                    .map_or(last_included_index, |v| v.index),
            }
        } else {
            debug!("{} Handle {}, Success true", self, args);
            self.log
                .truncate((args.prev_log_index - last_included_index) as usize);
            self.log.extend(args.entries);
            self.persist();
            if args.leader_commit > self.commit_index.load(Ordering::SeqCst) {
                self.commit_index
                    .store(min(args.leader_commit, self.last_index()), Ordering::SeqCst);
            }
            AppendEntriesReply {
                term: self.current_term.load(Ordering::SeqCst),
                success: true,
                conflict_log_index: 0,
                conflict_log_term: 0,
            }
        }
    }

    fn send_append_entries_all(&mut self) {
        // let mut rx_vec = FuturesUnordered::new();
        debug!("{} Send append entries to ALL RaftNode", self);
        let term = self.current_term.load(Ordering::SeqCst);
        // let peers_num = self.peers.len();
        for server in 0..self.peers.len() {
            if server != self.id {
                let match_index = self.match_index[server].clone();
                let next_index = self.next_index[server].clone();
                let tx = self.sender.clone();
                let is_leader = self.is_leader.clone();
                let last_included_index = self.last_included_index.load(Ordering::SeqCst);
                let last_included_term = self.last_included_term.load(Ordering::SeqCst);

                let prev_log_index = max(1, self.next_index[server].load(Ordering::SeqCst)) - 1;
                if prev_log_index < last_included_index {
                    let args = InstallSnapshotArgs {
                        term: self.current_term.load(Ordering::SeqCst),
                        leader_id: self.id as i32,
                        last_included_index,
                        last_included_term,
                        offset: 0,
                        data: self.persister.snapshot(),
                        done: true,
                    };
                    debug!("{} Send RaftNode {} {} ", self, server, args);
                    let rx = self.send_install_snapshot(server, args);
                    tokio::spawn(async move {
                        if let Ok(Ok(reply)) = rx.await {
                            if reply.term > term {
                                tx.send(RaftEvent::BecomeFollower(reply.term)).unwrap();
                            } else {
                                match_index.store(last_included_index, Ordering::SeqCst);
                                next_index.store(last_included_index + 1, Ordering::SeqCst);
                            }
                        }
                    });
                } else {
                    let prev_log_term = {
                        if prev_log_index == last_included_index {
                            last_included_term
                        } else {
                            // self.log[(prev_log_index - 1) as usize].term
                            self.log[(prev_log_index - last_included_index - 1) as usize].term
                        }
                    };
                    // let upper_log_index = min(prev_log_index + 5, self.log.len() as u64);
                    let upper_log_index = last_included_index + self.log.len() as u64;
                    let entries = {
                        if prev_log_index < upper_log_index {
                            self.log[((prev_log_index - last_included_index) as usize)
                                ..((upper_log_index - last_included_index) as usize)]
                                .to_vec()
                        } else {
                            Vec::new()
                        }
                    };
                    let args = AppendEntriesArgs {
                        term: self.current_term.load(Ordering::SeqCst),
                        leader_id: self.id as i32,
                        prev_log_index,
                        prev_log_term,
                        entries,
                        leader_commit: self.commit_index.load(Ordering::SeqCst),
                    };
                    debug!("{} Send RaftNode {} {} ", self, server, args);
                    // rx_vec.push(self.send_append_entries(server, args));
                    let rx = self.send_append_entries(server, args);
                    tokio::spawn(async move {
                        if let Ok(Ok(reply)) = rx.await {
                            if is_leader.load(Ordering::SeqCst) {
                                if !reply.success && reply.term > term {
                                    is_leader.store(false, Ordering::SeqCst);
                                    tx.send(RaftEvent::BecomeFollower(reply.term)).unwrap();
                                } else if reply.success {
                                    // info!("recv {}, upper: {}", reply, upper_log_index);
                                    match_index.store(upper_log_index, Ordering::SeqCst);
                                    next_index.store(upper_log_index + 1, Ordering::SeqCst);
                                } else {
                                    next_index.store(reply.conflict_log_index, Ordering::SeqCst);
                                }
                            }
                        }
                    });
                    self.update_commit_index();
                }
                self.update_commit_index();
            }
        }
    }
}

impl RaftCore {
    fn send_heart_beat(
        &self,
        server: usize,
        args: HeartBeatArgs,
    ) -> Receiver<Result<HeartBeatReply>> {
        let peer = &self.peers[server];
        let mut peer_clone = peer.clone();
        let (tx, rx) = channel::<Result<HeartBeatReply>>();
        tokio::spawn(async move {
            let res = peer_clone
                .heart_beat(Request::new(args))
                .await
                .map(|resp| resp.into_inner())
                .map_err(KvError::Rpc);
            let _ = tx.send(res);
        });
        rx
    }

    fn handle_heart_beat(&mut self, args: HeartBeatArgs) -> HeartBeatReply {
        let current_term = self.current_term.load(Ordering::SeqCst);
        if current_term < args.term {
            self.voted_for = Some(args.leader_id as usize);
            self.become_follower(args.term);
            debug!(
                "{} Become Follower. New Leader id: {}",
                self, args.leader_id
            );
        }
        let commit_index = self.commit_index.load(Ordering::SeqCst);
        if args.term < current_term || commit_index < args.leader_commit {
            HeartBeatReply {
                term: current_term,
                success: false,
                commit_index,
            }
        } else {
            HeartBeatReply {
                term: current_term,
                success: true,
                commit_index,
            }
        }
    }

    fn send_heart_beat_all(&mut self) {
        debug!("{} Send append entries to ALL RaftNode", self);
        let beat_count = Arc::new(AtomicUsize::new(1));
        let term = self.current_term.load(Ordering::SeqCst);
        let args = HeartBeatArgs {
            term,
            leader_id: self.id as i32,
            leader_commit: self.commit_index.load(Ordering::SeqCst),
        };
        for server in 0..self.peers.len() {
            if server != self.id {
                let args = args.clone();
                let tx = self.sender.clone();
                let peers_num = self.peers.len();
                let beat_count = beat_count.clone();
                let rx = self.send_heart_beat(server, args);
                tokio::spawn(async move {
                    if let Ok(reply) = rx.await {
                        if let Ok(reply) = reply {
                            if reply.term > term {
                                tx.send(RaftEvent::BecomeFollower(reply.term)).unwrap();
                            } else if reply.success {
                                beat_count.fetch_add(1, Ordering::Relaxed);
                                if beat_count.load(Ordering::SeqCst) > peers_num / 2 {
                                    tx.send(RaftEvent::ReadOnlyCommit(reply.commit_index))
                                        .unwrap();
                                }
                            }
                        }
                    }
                });
            }
        }
    }
}

impl RaftCore {
    fn send_install_snapshot(
        &self,
        server: usize,
        args: InstallSnapshotArgs,
    ) -> Receiver<Result<InstallSnapshotReply>> {
        let peer = &self.peers[server];
        let mut peer_clone = peer.clone();
        let (tx, rx) = channel::<Result<InstallSnapshotReply>>();
        tokio::spawn(async move {
            let res = peer_clone
                .install_snapshot(Request::new(args))
                .await
                .map(|resp| resp.into_inner())
                .map_err(KvError::Rpc);
            let _ = tx.send(res);
        });
        rx
    }

    fn handle_install_snapshot(&mut self, args: InstallSnapshotArgs) -> InstallSnapshotReply {
        let current_term = self.current_term.load(Ordering::SeqCst);
        if current_term < args.term {
            self.voted_for = Some(args.leader_id as usize);
            self.become_follower(args.term);
            debug!(
                "{} Become Follower. New Leader id: {}",
                self, args.leader_id
            );
        }
        let last_included_index = self.last_included_index.load(Ordering::SeqCst);
        if args.term == current_term && args.last_included_index > last_included_index {
            let range = min(
                self.log.len(),
                (args.last_included_index - last_included_index) as usize,
            );
            self.log.drain(..range);
            self.last_included_index
                .store(args.last_included_index, Ordering::SeqCst);
            self.last_included_term
                .store(args.last_included_term, Ordering::SeqCst);
            self.persist_with_snapshot(args.data.clone());
            self.commit_index
                .fetch_max(args.last_included_index, Ordering::SeqCst);
            self.last_applied
                .fetch_max(args.last_included_index, Ordering::SeqCst);

            let msg = ApplyMsg {
                command_valid: false,
                command: args.data,
                command_index: 0,
            };
            self.apply_ch.send(msg).expect("Unable send ApplyMsg");
        }

        InstallSnapshotReply { term: current_term }
    }
}

#[derive(Debug)]
enum RaftEvent {
    RequestVote(RequestVoteArgs, Sender<RequestVoteReply>),
    AppendEntries(AppendEntriesArgs, Sender<AppendEntriesReply>),
    HeartBeat(HeartBeatArgs, Sender<HeartBeatReply>),
    InstallSnapshot(InstallSnapshotArgs, Sender<InstallSnapshotReply>),
    BecomeLeader(u64),
    BecomeFollower(u64),
    ReadOnlyCommit(u64),
    StartCommand(Vec<u8>, Sender<Result<(u64, u64)>>),
    StartReadOnly(Vec<u8>, Sender<Result<(u64, u64)>>),
    StartSnapshot(Vec<u8>, u64),
    Shutdown,
}

impl Stream for RaftCore {
    type Item = ();

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        trace!("{} poll event!", self);
        match self.timeout.poll_unpin(cx) {
            Poll::Ready(()) => {
                return {
                    trace!("{} poll timeout ready!", self);
                    if self.is_leader.load(Ordering::SeqCst) {
                        self.timeout.reset(heartbeat_timeout());
                        self.send_append_entries_all();
                        Poll::Ready(Some(()))
                    } else {
                        trace!("{} loss Leader connection", self);
                        self.timeout.reset(election_timeout());
                        self.become_candidate();
                        Poll::Ready(Some(()))
                    }
                };
            }
            Poll::Pending => {}
        };
        match self.apply_msg_delay.poll_unpin(cx) {
            Poll::Ready(()) => {
                trace!("{} poll Apply Msg ready!", self);
                self.apply_msg_delay.reset(heartbeat_timeout());
                self.send_apply_msg();
                return Poll::Ready(Some(()));
            }
            Poll::Pending => {}
        };
        match self.receiver.poll_recv(cx) {
            Poll::Ready(Some(event)) => match event {
                RaftEvent::RequestVote(args, tx) => {
                    let reply = self.handle_request_vote(args);
                    if reply.vote_granted {
                        self.timeout.reset(election_timeout());
                    }
                    let _ = tx.send(reply);
                    Poll::Ready(Some(()))
                }
                RaftEvent::AppendEntries(args, tx) => {
                    let current_term = args.term;
                    let reply = self.handle_append_entries(args);
                    if reply.success || reply.term == current_term {
                        self.timeout.reset(election_timeout());
                    }
                    let _ = tx.send(reply);
                    Poll::Ready(Some(()))
                }
                RaftEvent::HeartBeat(args, tx) => {
                    let current_term = args.term;
                    let reply = self.handle_heart_beat(args);
                    if reply.success || reply.term == current_term {
                        self.timeout.reset(election_timeout());
                    }
                    let _ = tx.send(reply);
                    Poll::Ready(Some(()))
                }
                RaftEvent::InstallSnapshot(args, tx) => {
                    let reply = self.handle_install_snapshot(args);
                    let _ = tx.send(reply);
                    Poll::Ready(Some(()))
                }
                RaftEvent::BecomeLeader(term) => {
                    self.become_leader(term);
                    self.timeout.reset(heartbeat_timeout());
                    self.start(&Vec::new()).unwrap();
                    self.send_append_entries_all();
                    Poll::Ready(Some(()))
                }
                RaftEvent::BecomeFollower(term) => {
                    self.become_follower(term);
                    self.timeout.reset(election_timeout());
                    Poll::Ready(Some(()))
                }
                RaftEvent::ReadOnlyCommit(commit) => {
                    let msgs = self.read_only.pop_requests(commit);
                    self.apply_read_only(commit, msgs);
                    Poll::Ready(Some(()))
                }
                RaftEvent::StartCommand(command, tx) => {
                    debug!("{} Exexutor -- Receive command!", self);
                    let _ = tx.send(self.start(&command));
                    Poll::Ready(Some(()))
                }
                RaftEvent::StartReadOnly(command, tx) => {
                    debug!("{} Exexutor -- Receive command!", self);
                    let _ = tx.send(self.start_read_only(&command));
                    Poll::Ready(Some(()))
                }
                RaftEvent::StartSnapshot(snapshot, last_applied) => {
                    let snapshot_len =
                        (last_applied - self.last_included_index.load(Ordering::SeqCst)) as usize;
                    if snapshot_len > 0 {
                        info!("{} Exexutor -- Receive Snapshot!", self);
                        self.last_included_index
                            .store(last_applied, Ordering::SeqCst);
                        self.last_included_term
                            .store(self.log[snapshot_len - 1].term, Ordering::SeqCst);
                        self.log.drain(..snapshot_len);
                        self.persist_with_snapshot(snapshot);
                        info!("{} Exexutor -- Finish Snapshot!", self);
                    }
                    Poll::Ready(Some(()))
                }
                RaftEvent::Shutdown => Poll::Ready(None),
            },
            Poll::Ready(None) => Poll::Ready(Some(())),
            Poll::Pending => Poll::Pending,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}

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
