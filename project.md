# Key/Value Storage System

This project is addapted from [Talent Plan](https://github.com/pingcap/talent-plan/tree/master/courses/dss) from [PingCAP University](https://university.pingcap.com/). Because it is addapted from an online course, this project contains some starter code. The original part is to add some more features including `Raft` consensus, `Percolator` transaction, and RPC operations

## Feature

- Writen with Rust
- Support transaction command:
    - **Set**: add a Key/Value pairs into storage
    - **Get**: get the value with given key
    - **Remove**: remove Key/Value pairs from storage
- Backend storage engine:
    - `KvSled`: Internally uses sled, a key-value storage engine library, to persist data to local disk[`sled`](https://github.com/spacejam/sled)
- Multiple server kinds:
    - `basic`: use a single server to handle requests, supports `Percolator` transaction
    - `raft`: use multiple raft nodes as a whole server, supports `Percolator` transaction also
- Support RPC: (based on `tonic` crate)
    - `KvRpc`: a RPC that interacts with client, supports transaction command
    - `RaftRpc`: a RPC which used between raft nodes, supports leader election, heartbeat, append entries, and install snapshot, which followed directly from
- Support Percolator
	Percolator’s transactions are committed by a 2-phase commit (2PC) algorithm. Its two phases are Prewrite and Commit.

	In `Prewrite` phase:

	Get a timestamp from the timestamp oracle, and we call the timestamp start_ts of the transaction.
	For each row involved in the transaction, put a lock in the lock column and write the value to the data column with the timestamp start_ts. One of these locks will be chosen as the primary lock, while others are secondary locks. Each lock contains the transaction’s start_ts. Each secondary lock, in addition, contains the location of the primary lock.
	If there’s already a lock or newer version than start_ts, the current transaction will be rolled back because of write conflict.
	And then, in the `Commit` phase:

	Get another timestamp, namely commit_ts.
	Remove the primary lock, and at the same time write a record to the write column with timestamp commit_ts, whose value records the transaction’s start_ts. If the primary lock is missing, the commit fails.
	Repeat the process above for all secondary locks.
	Once step 2 (committing the primary) is done, the whole transaction is done. It doesn’t matter if the process of committing the secondary locks failed.

### Use binary version
For now we can simulate distributed system with multiple process on a single node.
```sh
./kvs-server
```
Run a single raft node with multiple know peer raft nodes
```sh
./kvs-server -e sled -s raft -i 0 -a "127.0.0.1:5000" "127.0.0.1:5001" "127.0.0.1:5002" "127.0.0.1:5003" "127.0.0.1:5004"
```
where `-i <num>` is the index into the list of addresses.
```sh
./kvs-server -e -s raft -a
-a "127.0.0.1:5000" "127.0.0.1:5001" "127.0.0.1:5002" "127.0.0.1:5003" "127.0.0.1:5004"
```

And we can access these server via the client
```sh
./kvs-client set key val -a "127.0.0.1:5000" "127.0.0.1:5001" "127.0.0.1:5002" "127.0.0.1:5003" "127.0.0.1:5004"
```

### Use Server from code

For a server, you can use its `builder` to create a factory to specify some message:

- backend engine, which is choosen to be `sled`
- the server kind, which can be `basic` or `raft`
- server directory path, which can be set with `set_root_path`. It will make all server node files save in this root directory, for different server, its path will be `root_path/server-{i}` (`i` is its index). It is useful to create many server without specify all node's path. You can alse specify each server with a specific path with `add_node` function,
- listening address, which can be set with `add_batch_nodes` with a vector of addr or `add_node` with a single addr

After that, you can use `build` to get the server instance. `server.start()` will start this server by listening on specified address.

You can find [`src/bin/kvs-server.rs`](src/bin/kvs-server.rs) to see more details.

```rs
#[tokio::main]
async fn main() -> Result<()> {
    let addrs = vec!["127.0.0.1:4000", "127.0.0.1:4001", "127.0.0.1:4002"];
    let server = KvsServer::builder()
        .set_engine("sled")
        .set_server("raft")
        .set_root_path(current_dir().unwrap())
        .add_batch_nodes(addrs);
        .build();

    server.start()
}
```

### Client

For a client, it need to know all the server address which will be used. Likewise, you can use `add_batch_nodes` to add a batch of node addresses, or you can use `add_node` to add a node address. A little difference is client does not need to set the directory path.

After that you can start a transaction (`txn_start`), get some value of specific key (`txn_get`), set or delete some key value pairs (`txn_set`, `txn_delete`) and commit these change you made (`txn_commit`).

There are some other useful methods for getting (`get`), setting (`set`) and deleting (`remove`) one key, if you just want to make one command, you can use them for convenience.

You can find [`src/bin/kvs-client.rs`](src/bin/kvs-client.rs) to see more details.

```rs
#[tokio::main]
async fn main() -> Result<()> {
    let addrs = vec!["127.0.0.1:4000", "127.0.0.1:4001", "127.0.0.1:4002"];
    let mut client = KvsClient::builder()
        .add_batch_nodes(addrs)
        .build();
    client.txn_start().await?;
    client.txn_get("key").await?;
    client.txn_set("key", "value")?;
    client.txn_commit().await?
}
```