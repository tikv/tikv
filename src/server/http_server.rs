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


use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::io::{Read, Write};
use std::sync::mpsc::{self, Sender, Receiver};

use uuid::Uuid;
use protobuf::Message;
use mio::tcp::TcpListener;
use hyper::{Next, Encoder, Decoder, Control};
use hyper::method::Method::{Post, Put};
use hyper::server::{Server as HyperServer, Handler as HyperHandler, Request, Response, Listening};
use hyper::net::{HttpListener, Transport as HyperTransport};
use hyper::{RequestUri, StatusCode};
use hyper::header::ContentLength;

use kvproto::msgpb;
use super::Result;
use super::http::{Body, OnResponseResult, OnResponse, XRequestGuid};

const HTTP_CONN_IDLE_TIMEOUT_MS: u64 = 30000;

pub trait ServerHandler: Send + Sync {
    fn on_request(&mut self, msg: msgpb::Message, cb: OnResponse);
}

pub const V1_MSG_PATH: &'static str = "/api/v1/msg";

#[derive(Debug)]
enum Route {
    Invalid {
        // TODO: add errorpb, so that we can return detail error in body.
        code: StatusCode,
    },
    Msg,
}

struct Handler<T: ServerHandler + 'static> {
    h: Arc<Mutex<T>>,

    ctrl: Arc<Mutex<Control>>,

    route: Route,

    request_body: Body,
    response_body: Body,

    tx: Sender<OnResponseResult>,
    rx: Receiver<OnResponseResult>,

    tag: String,
}

impl<T: ServerHandler> Handler<T> {
    pub fn new(h: Arc<Mutex<T>>, ctrl: Control) -> Handler<T> {
        let (tx, rx) = mpsc::channel();
        Handler {
            h: h,
            ctrl: Arc::new(Mutex::new(ctrl)),
            route: Route::Invalid { code: StatusCode::NotFound },

            request_body: Body::default(),
            response_body: Body::default(),

            tx: tx,
            rx: rx,

            tag: "".to_owned(),
        }
    }
}



impl<T: ServerHandler, H: HyperTransport> HyperHandler<H> for Handler<T> {
    fn on_request(&mut self, request: Request) -> Next {
        let uuid = match request.headers().get::<XRequestGuid>() {
            Some(uuid) => uuid.0.clone(),
            None => {
                warn!("missing request guid header, create one");
                Uuid::new_v4().simple().to_string()
            }
        };

        self.tag = format!("[S:{}]", uuid);

        debug!("{} on request {} {:?}",
               self.tag,
               request.method(),
               request.uri());
        // Now we only support POST|PUT /v1/msg
        match *request.uri() {
            RequestUri::AbsolutePath(ref path) => {
                match (request.method(), &path[..]) {
                    (&Post, V1_MSG_PATH) |
                    (&Put, V1_MSG_PATH) => {
                        if let Some(len) = request.headers().get::<ContentLength>() {
                            // Must have Content-Length header
                            self.route = Route::Msg;
                            self.request_body.reset(**len as usize);
                            Next::read()
                        } else {
                            Next::write()
                        }
                    }
                    _ => Next::write(),
                }
            }
            _ => Next::write(),
        }
    }
    fn on_request_readable(&mut self, decoder: &mut Decoder<H>) -> Next {
        debug!("{} on request readable for route {:?}",
               self.tag,
               self.route);

        match self.route {
            Route::Msg => {
                if let Err(e) = self.request_body.read_from(decoder) {
                    error!("{} read body failed: {:?}", self.tag, e);
                    return Next::remove();
                }

                if self.request_body.remaining() > 0 {
                    // need read more data
                    return Next::read();
                }

                let mut msg = msgpb::Message::new();
                if let Err(e) = msg.merge_from_bytes(self.request_body.as_bytes()) {
                    error!("{} decode message failed {:?}", self.tag, e);
                    self.route = Route::Invalid { code: StatusCode::InternalServerError };
                    return Next::write();
                }

                let ctrl = self.ctrl.clone();
                let tx = self.tx.clone();
                let mut h = self.h.lock().unwrap();
                h.on_request(msg,
                             box move |res| {
                                 let ctrl = ctrl.lock()
                                                .unwrap();
                                 if let Err(e) = tx.send(res) {
                                     error!("send response failed {:?}", e);
                                     if let Err(e) = ctrl.ready(Next::end()) {
                                         error!("set response writable to END failed {:?}", e);
                                     }
                                     return;
                                 }

                                 if let Err(e) = ctrl.ready(Next::write()) {
                                     error!("set response writable to WRITE failed {:?}", e);
                                 }
                             });
                Next::wait()
            }
            _ => unreachable!(),
        }
    }
    fn on_response(&mut self, response: &mut Response) -> Next {
        debug!("{} on response for route {:?}", self.tag, self.route);

        match self.route {
            Route::Invalid { code } => {
                response.set_status(code);
                Next::end()
            }
            Route::Msg => {
                // For valid logic, we send response message first and then
                // ready Writable, so here we must get the response.
                match self.rx.try_recv() {
                    Err(e) => {
                        error!("{} get response error {:?}", self.tag, e);
                        response.set_status(StatusCode::InternalServerError);
                        Next::end()
                    }
                    Ok(res) => {
                        match res {
                            Err(e) => {
                                error!("{} handle request error {:?}", self.tag, e);
                                response.set_status(StatusCode::InternalServerError);
                                Next::end()
                            }
                            Ok(None) => Next::end(),
                            Ok(Some(msg)) => {
                                self.response_body.reset(0);
                                // must write OK.
                                msg.write_to_vec(self.response_body.as_mut()).unwrap();

                                response.headers_mut()
                                        .set(ContentLength(self.response_body.len() as u64));
                                Next::write()
                            }
                        }
                    }

                }
            }
        }
    }
    fn on_response_writable(&mut self, encoder: &mut Encoder<H>) -> Next {
        debug!("{} on response writable for route {:?}",
               self.tag,
               self.route);

        match self.route {
            Route::Msg => {
                if let Err(e) = self.response_body.write_to(encoder) {
                    error!("{} write response body failed {:?}", self.tag, e);
                    return Next::remove();
                }

                if self.response_body.remaining() > 0 {
                    // need write more data.
                    Next::write()
                } else {
                    // write all OK
                    Next::end()
                }
            }
            _ => unreachable!(),
        }
    }
}

pub struct Server<T: ServerHandler + 'static> {
    h: Arc<Mutex<T>>,
}

impl<T: ServerHandler> Server<T> {
    pub fn new(h: T) -> Server<T> {
        Server { h: Arc::new(Mutex::new(h)) }
    }
}

impl<T: ServerHandler> Server<T> {
    pub fn run(&mut self, listener: TcpListener) -> Result<Listening> {
        // TODO: support HTTPS later.
        let s = HyperServer::new(HttpListener(listener))
                    .keep_alive(true)
                    .idle_timeout(Duration::from_millis(HTTP_CONN_IDLE_TIMEOUT_MS));

        let h = self.h.clone();
        let listening = try!(s.handle(move |ctrl| Handler::new(h.clone(), ctrl)));

        Ok(listening)
    }
}
