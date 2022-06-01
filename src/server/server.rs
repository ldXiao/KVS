// use crate::rpc::KvStoreService;
use crate::*;
use crate::{backend::EngineKind, thread_pool::*};
// use futures::future::Ready;
use std::{
    io::Write,
    io::{prelude::*, BufReader, BufWriter},
    net::SocketAddr,
    net::{TcpListener, TcpStream},
    str::from_utf8,
};

// use tonic::{transport::Server, Request, Response, Status};

// pub mod kvs_server_service {
//     tonic::include_proto!("kvserver");
// }
// use kvs_server_service::kv_server_server::{KvServer, KvServerServer};
// use kvs_server_service::{GetReply, GetRequest, RemoveReply, RemoveRequest, SetReply, SetRequest};

/// Kvs Server
pub struct KvsServer {
    store: EngineKind,
    thread_pool: ThreadPoolKind,
}

impl KvsServer {
    /// Construct a new Kvs Server from given engine at specific path.
    /// Use `run()` to listen on given addr.
    pub fn new(store: EngineKind, thread_pool: ThreadPoolKind) -> Result<Self> {
        Ok(KvsServer { store, thread_pool })
    }
    /// Run Kvs Server at given Addr
    pub fn run(&mut self, addr: SocketAddr) -> Result<()> {
        let listener = TcpListener::bind(addr)?;

        info!("[Server] Listening on {}", addr);

        // accept connections and process them serially
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let store = self.store.clone();
                    self.thread_pool.spawn(move || {
                        handle_request(store, stream).unwrap();
                    })
                }
                Err(e) => println!("{}", e),
            }
        }
        Ok(())
    }
}

fn handle_request(store: EngineKind, stream: TcpStream) -> Result<()> {
    let mut reader = BufReader::new(&stream);
    let mut writer = BufWriter::new(&stream);

    let mut buf = vec![];
    let _len = reader.read_until(b'}', &mut buf)?;
    let request_str = from_utf8(&buf).unwrap();

    let request: Request = serde_json::from_str(request_str)?;
    let response = process_request(store, request);

    let response_str = serde_json::to_string(&response)?;
    writer.write(&response_str.as_bytes())?;
    writer.flush()?;

    info!(
        "[Server] Received request from {} - Args: {}, Response: {}",
        stream.local_addr()?,
        request_str,
        response_str
    );

    Ok(())
}

fn process_request(store: EngineKind, req: Request) -> Response {
    match req.cmd.as_str() {
        "Get" => match store.get(req.key) {
            Ok(Some(value)) => Response {
                status: "ok".to_string(),
                result: Some(value),
            },
            Ok(None) => Response {
                status: "ok".to_string(),
                result: Some("Key not found".to_string()),
            },
            Err(_) => Response {
                status: "err".to_string(),
                result: Some("Something Wrong!".to_string()),
            },
        },
        "Set" => match store.set(req.key, req.value.unwrap()) {
            Ok(_) => Response {
                status: "ok".to_string(),
                result: None,
            },
            Err(_) => Response {
                status: "err".to_string(),
                result: Some("Set Error!".to_string()),
            },
        },
        "Remove" => match store.remove(req.key) {
            Ok(_) => Response {
                status: "ok".to_string(),
                result: None,
            },
            Err(_) => Response {
                status: "err".to_string(),
                result: Some("Key not found".to_string()),
            },
        },
        _ => Response {
            status: "err".to_string(),
            result: Some("Unknown Command!".to_string()),
        },
    }
}

// impl<E: KvsEngine, P: ThreadPool> KvServer for KvsServer<E, P> {}