// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::{Arc, RwLock};
use std::boxed::{Box, FnBox};
use std::net::SocketAddr;
use std::collections::HashMap;
use std::time::Duration;
use std::thread::{Builder, JoinHandle};
use std::collections::HashSet;

use c_ares::{flags, Socket, Channel, Options, SOCKET_BAD, AddressFamily};
use mio::{EventLoop, Handler, Token, EventSet, PollOpt, Sender};
// TODO: simply support windows later.
use mio::unix::EventedFd;

use super::{Result, send_msg};
use util::{HandyRwLock, TryInsertWith};
use pd::PdClient;

pub type Callback = Box<FnBox(Result<SocketAddr>) + Send>;

const ARES_PROCESS_TIMEOUT: u64 = 500;
const ARES_MAX_TRIES: u32 = 3;
const ARES_QUERY_TIMEOUT_MS: u32 = 500;

// StoreAddrResolver resolves the store address.
pub trait StoreAddrResolver {
    // Resolve resolves the store address asynchronously.
    fn resolve(&self, store_id: u64, cb: Callback) -> Result<()>;
}

struct PdStoreAddrPeek<T: PdClient> {
    cluster_id: u64,
    pd_client: Arc<RwLock<T>>,
    store_addrs: HashMap<u64, String>,
}

impl<T: PdClient> PdStoreAddrPeek<T> {
    pub fn new(cluster_id: u64, pd_client: Arc<RwLock<T>>) -> PdStoreAddrPeek<T> {
        PdStoreAddrPeek {
            cluster_id: cluster_id,
            pd_client: pd_client,
            store_addrs: HashMap::new(),
        }
    }

    pub fn get_address(&mut self, store_id: u64) -> Result<&str> {
        // TODO: do we need re-update the cache sometimes?
        // Store address may be changed?
        let pd_client = self.pd_client.clone();
        let cluster_id = self.cluster_id;
        let s = try!(self.store_addrs.entry(store_id).or_try_insert_with(|| {
            pd_client.rl()
                     .get_store(cluster_id, store_id)
                     .and_then(|s| {
                         let addr = s.get_address().to_owned();
                         // In some tests, we use empty address for store first,
                         // so we should ignore here.
                         // TODO: we may remove this check after we refactor the test.
                         if addr.len() == 0 {
                             return Err(box_err!("invalid empty address for store {}", store_id));
                         }
                         Ok(addr)
                     })
        }));

        Ok(s)
    }
}

enum Msg {
    Resolve {
        store_id: u64,
        cb: Callback,
    },
    AresCallback {
        sock: Socket,
        readable: bool,
        writable: bool,
    },
    Quit,
}

struct PdResolveWorker<T: PdClient> {
    peeker: PdStoreAddrPeek<T>,
    channel: Channel,
    sockets: HashSet<Socket>,
}

impl<T: PdClient> PdResolveWorker<T> {
    pub fn new(cluster_id: u64,
               pd_client: Arc<RwLock<T>>,
               ch: Sender<Msg>)
               -> Result<PdResolveWorker<T>> {
        let mut options = Options::new();

        options.set_socket_state_callback(move |sock: Socket, readable: bool, writable: bool| {
                   if let Err(e) = send_msg(&ch,
                                            Msg::AresCallback {
                                                sock: sock,
                                                readable: readable,
                                                writable: writable,
                                            }) {
                       error!("send ares callback msg failed {:?}", e);
                   }
               })
               .set_flags(flags::STAYOPEN | flags::EDNS)
               .set_timeout(ARES_QUERY_TIMEOUT_MS)
               .set_tries(ARES_MAX_TRIES);

        let channel = box_try!(Channel::new(options));

        Ok(PdResolveWorker {
            peeker: PdStoreAddrPeek::new(cluster_id, pd_client),
            channel: channel,
            sockets: HashSet::new(),
        })
    }

    fn on_resolve(&mut self, _: &mut EventLoop<Self>, store_id: u64, cb: Callback) {
        // TODO: use asynchronously store address get with pd too.
        let addr = match self.peeker.get_address(store_id) {
            Err(e) => {
                return cb.call_box((Err(box_err!("get store {} address failed {:?}",
                                                 store_id,
                                                 e)),))
            }
            Ok(addr) => addr,
        };

        if let Ok(sock) = addr.parse() {
            // parse ok, the format is ip:port.
            return cb.call_box((Ok(sock),));
        }

        // The format is host:port here.
        let seps: Vec<&str> = addr.split(':').collect();
        if seps.len() != 2 {
            return cb.call_box((Err(box_err!("invalid address format {}", addr)),));
        }

        let host = seps[0].to_owned();
        let port: u16 = match seps[1].parse() {
            Err(e) => return cb.call_box((Err(box_err!("parse port {} failed {:?}", seps[1], e)),)),
            Ok(port) => port,
        };

        self.channel.get_host_by_name(seps[0], AddressFamily::INET, move |res| {
            match res {
                Err(e) => cb.call_box((Err(box_err!("get host {} failed {:?}", host, e)),)),
                Ok(host_res) => {
                    for addr in host_res.addresses() {
                        let sock = SocketAddr::new(addr, port);
                        return cb.call_box((Ok(sock),));
                    }

                    // here means we get nothing
                    cb.call_box((Err(box_err!("get host {} failed", host)),))
                }
            }
        })
    }

    fn on_ares_callback(&mut self,
                        event_loop: &mut EventLoop<Self>,
                        sock: Socket,
                        readable: bool,
                        writable: bool) {
        let efd = EventedFd(&sock);
        let token = Token(sock as usize);

        if !readable && !writable {
            self.sockets.remove(&sock);
            if let Err(e) = event_loop.deregister(&efd) {
                error!("deregister resolve socket token {:?} failed {:?}", token, e);
            }

            return;
        }

        let mut interest = EventSet::none();
        if readable {
            interest.insert(EventSet::readable());
        }

        if writable {
            interest.insert(EventSet::writable());
        }

        if let Err(e) = if self.sockets.insert(sock) {
            event_loop.register(&efd, token, interest, PollOpt::edge())
        } else {
            event_loop.reregister(&efd, token, interest, PollOpt::edge())
        } {
            error!("register resolve socket token {:?} failed {:?}", token, e)
        }
    }
}

impl<T: PdClient> Handler for PdResolveWorker<T> {
    type Timeout = ();
    type Message = Msg;

    fn ready(&mut self, _: &mut EventLoop<Self>, token: Token, events: EventSet) {
        let fd = token.as_usize() as Socket;
        let read_fd = if events.is_readable() {
            fd
        } else {
            SOCKET_BAD
        };

        let write_fd = if events.is_writable() {
            fd
        } else {
            SOCKET_BAD
        };

        self.channel.process_fd(read_fd, write_fd);
    }

    fn notify(&mut self, event_loop: &mut EventLoop<Self>, msg: Self::Message) {
        match msg {
            Msg::Quit => {
                info!("quitting pd resolve worker");
                event_loop.shutdown();
            }
            Msg::Resolve { store_id, cb } => self.on_resolve(event_loop, store_id, cb),
            Msg::AresCallback { sock, readable, writable } => {
                self.on_ares_callback(event_loop, sock, readable, writable)
            }
        }
    }

    fn timeout(&mut self, event_loop: &mut EventLoop<Self>, _: Self::Timeout) {
        self.channel.process_fd(SOCKET_BAD, SOCKET_BAD);
        event_loop.timeout((), Duration::from_millis(ARES_PROCESS_TIMEOUT))
                  .expect("must register ares process timeout");
    }
}


pub struct PdStoreAddrResolver {
    ch: Sender<Msg>,
    resolve_handle: Option<JoinHandle<()>>,
}

impl PdStoreAddrResolver {
    pub fn new<T>(cluster_id: u64, pd_client: Arc<RwLock<T>>) -> Result<PdStoreAddrResolver>
        where T: PdClient + 'static
    {
        let mut event_loop = try!(EventLoop::new());
        let ch = event_loop.channel();

        let mut worker = try!(PdResolveWorker::new(cluster_id, pd_client, ch.clone()));

        let builder = Builder::new().name("store address resolve worker".to_owned());
        let h = try!(builder.spawn(move || {
            event_loop.timeout((), Duration::from_millis(ARES_PROCESS_TIMEOUT))
                      .expect("must register ares process timeout");

            if let Err(e) = event_loop.run(&mut worker) {
                error!("run resolve thread err {:?}", e);
            };
        }));

        Ok(PdStoreAddrResolver {
            ch: ch,
            resolve_handle: Some(h),
        })
    }
}

impl StoreAddrResolver for PdStoreAddrResolver {
    fn resolve(&self, store_id: u64, cb: Callback) -> Result<()> {
        try!(send_msg(&self.ch,
                      Msg::Resolve {
                          store_id: store_id,
                          cb: cb,
                      }));
        Ok(())
    }
}

impl Drop for PdStoreAddrResolver {
    fn drop(&mut self) {
        if let Err(e) = self.ch.send(Msg::Quit) {
            error!("failed to stop store address resolve thread: {:?}!!!", e);
            return;
        }

        let h = self.resolve_handle.take().unwrap();

        if let Err(e) = h.join() {
            error!("join resolve thread err {:?}", e);
        }
    }
}

pub struct MockStoreAddrResolver;

impl StoreAddrResolver for MockStoreAddrResolver {
    fn resolve(&self, _: u64, _: Callback) -> Result<()> {
        unimplemented!();
    }
}
