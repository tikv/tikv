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

use std::thread::{self, JoinHandle};
use std::sync::mpsc::{self, Sender};
use std::boxed::{Box, FnBox};
use std::net::SocketAddr;
use std::io;

use super::Result;
use util;

pub type Callback = Box<FnBox(io::Result<SocketAddr>) + Send>;

enum Msg {
    Peer {
        addr: String,
        cb: Callback,
    },
    Quit,
}

pub struct Resolver {
    tx: Sender<Msg>,
    h: Option<JoinHandle<()>>,
}

impl Resolver {
    pub fn new() -> Result<Resolver> {
        let (tx, rx) = mpsc::channel();
        let builder = thread::Builder::new().name("host-resolver".to_owned());
        let h = try!(builder.spawn(move || {
            while let Ok(Msg::Peer { addr, cb }) = rx.recv() {
                let resp = util::to_socket_addr(&*addr);
                cb.call_box((resp,));
            }
        }));

        Ok(Resolver {
            tx: tx,
            h: Some(h),
        })
    }

    pub fn resolve(&self, addr: String, cb: Callback) -> Result<()> {
        self.tx
            .send(Msg::Peer {
                addr: addr,
                cb: cb,
            })
            .unwrap();
        Ok(())
    }
}

impl Drop for Resolver {
    fn drop(&mut self) {
        if let Err(e) = self.tx.send(Msg::Quit) {
            error!("try to quit host resolver thread err {:?}", e);
            return;
        }

        let h = self.h.take().unwrap();
        if let Err(e) = h.join() {
            error!("join host resolver thread err {:?}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use super::*;

    #[test]
    fn test_resolver() {
        let (tx, rx) = mpsc::channel();
        let r = Resolver::new().unwrap();
        let cb = box move |r| {
            tx.send(r).unwrap();
        };
        r.resolve("localhost:80".to_owned(), cb).unwrap();
        rx.recv().unwrap().unwrap();
    }
}
