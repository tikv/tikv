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
use std::boxed::Box;
use kvproto::msgpb::{MessageType, Message};
use util::HandyRwLock;
use storage::Storage;
use super::kv::StoreHandler;
use super::coprocessor::EndPointHost;
use super::transport::RaftStoreRouter;

use super::http_server::ServerHandler;
use super::http::OnResponse as OnHttpResponse;

pub struct Handler<T: RaftStoreRouter + 'static> {
    raft_router: Arc<RwLock<T>>,

    store: StoreHandler,
    end_point: EndPointHost,
}

impl<T: RaftStoreRouter> Handler<T> {
    pub fn new(storage: Storage, raft_router: Arc<RwLock<T>>) -> Handler<T> {

        let engine = storage.get_engine();
        let store_handler = StoreHandler::new(storage);
        let end_point = EndPointHost::new(engine);

        Handler {
            raft_router: raft_router,
            store: store_handler,
            end_point: end_point,
        }
    }
}

impl<T: RaftStoreRouter> ServerHandler for Handler<T> {
    fn on_request(&mut self, mut msg: Message, cb: OnHttpResponse) {
        let msg_type = msg.get_msg_type();
        match msg_type {
            MessageType::Raft => {
                self.raft_router.rl().send_raft_msg(msg.take_raft()).unwrap();
                // Raft has no response.
                cb.call_box((Ok(None),))
            }
            MessageType::Cmd => {
                self.raft_router
                    .rl()
                    .send_command(msg.take_cmd_req(),
                                  box move |resp| {
                                      let mut resp_msg = Message::new();
                                      resp_msg.set_msg_type(MessageType::CmdResp);
                                      resp_msg.set_cmd_resp(resp);

                                      cb.call_box((Ok(Some(resp_msg)),));
                                      Ok(())
                                  })
                    .unwrap();
            }
            MessageType::KvReq => {
                self.store
                    .on_request(msg.take_kv_req(),
                                box move |resp: Message| cb.call_box((Ok(Some(resp)),)))
                    .unwrap();
            }
            MessageType::CopReq => {
                self.end_point.on_request(msg.take_cop_req(),
                                          box move |resp: Message| {
                                              cb.call_box((Ok(Some(resp)),))
                                          });
            }
            _ => cb.call_box((Err(box_err!("unsupported message {:?} ", msg_type)),)),
        }
    }
}
