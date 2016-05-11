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

use std::fmt::{self, Formatter, Debug};
use std::time::Duration;

use uuid::Uuid;
use hyper::client::{Request, Response, Handler as HyperHandler, HttpConnector};
use hyper::header::ContentLength;
use hyper::{Decoder, Encoder, Next};
use hyper::net::Transport as HyperTransport;
use hyper::{Get, Post, StatusCode, Error as HyperError};
use hyper::Client as HyperClient;
pub use hyper::Url;

use protobuf::Message;
use kvproto::msgpb;
use util::event::Event;

use super::http::{Body, OnResponseResult, OnResponse, XRequestGuid};
use super::{Result, Error};

// We only need to support:
// 1. Request message with remote store/pd, all the body is protobuf format.
// 2. Post Snapshot data to remote store. (Now we can use Post Message instead).

struct Msg {
    request_body: Body,
    response_body: Body,
    cb: Option<OnResponse>,
}

impl Drop for Msg {
    fn drop(&mut self) {
        // This is the last chance for use to call the callback if
        // we forget to call it.
        if self.cb.is_some() {
            error!("forget to call callback");
            let _ = self.on_end(Err(box_err!("forget to call callback")));
        }
    }
}

impl Debug for Msg {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "send message request")
    }
}

impl Msg {
    pub fn new(msg: Option<msgpb::Message>, cb: OnResponse) -> Msg {
        let mut pm = Msg {
            request_body: Body::default(),
            response_body: Body::default(),
            cb: Some(cb),
        };

        if let Some(msg) = msg {
            // must OK.
            msg.write_to_vec(pm.request_body.as_mut()).unwrap();
        }

        pm
    }

    fn on_end(&mut self, res: OnResponseResult) -> Next {
        let next = if res.is_ok() {
            Next::end()
        } else {
            Next::remove()
        };

        if let Some(cb) = self.cb.take() {
            cb.call_box((res,))
        }

        next
    }

    fn on_request(&mut self, req: &mut Request) -> Next {
        if self.request_body.is_empty() {
            req.set_method(Get);
            Next::read()
        } else {
            req.set_method(Post);
            req.headers_mut().set(ContentLength(self.request_body.len() as u64));
            Next::write()
        }
    }

    fn on_request_writable<H: HyperTransport>(&mut self, encoder: &mut Encoder<H>) -> Next {
        if let Err(e) = self.request_body.write_to(encoder) {
            error!("send request body failed {:?}", e);
            return self.on_end(Err(e));
        }

        if self.request_body.remaining() > 0 {
            Next::write()
        } else {
            Next::read()
        }
    }

    fn on_response(&mut self, res: Response) -> Next {
        if *res.status() != StatusCode::Ok {
            // TODO: check error response later.
            return self.on_end(Err(Error::HttpResponse(*res.status())));
        }

        if let Some(len) = res.headers().get::<ContentLength>() {
            self.response_body.reset(**len as usize);
            Next::read()
        } else {
            // Here no body
            self.on_end(Ok(None))
        }
    }

    fn on_response_readable<H: HyperTransport>(&mut self, decoder: &mut Decoder<H>) -> Next {
        if let Err(e) = self.response_body.read_from(decoder) {
            error!("get response body failed {:?}", e);
            return self.on_end(Err(e));
        }

        if self.response_body.remaining() > 0 {
            return Next::read();
        }

        let mut msg = msgpb::Message::new();
        if let Err(e) = msg.merge_from_bytes(self.response_body.as_bytes()).map_err(From::from) {
            error!("decode message failed {:?}", e);
            return self.on_end(Err(e));
        }

        self.on_end(Ok(Some(msg)))
    }

    fn on_error(&mut self, err: HyperError) -> Next {
        self.on_end(Err(Error::Hyper(err)))
    }
}

enum Route {
    Msg {
        msg: Msg,
    },
    // support later.
    _Snapshot,
}

impl Debug for Route {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Route::Msg { .. } => write!(f, "Msg"),
            _ => unimplemented!(),
        }
    }
}

struct Handler {
    uuid: Uuid,
    tag: String,
    route: Route,
}

impl Handler {
    pub fn new(route: Route) -> Handler {
        let uuid = Uuid::new_v4();
        let tag = format!("[C:{}]", uuid.simple());

        Handler {
            uuid: uuid,
            tag: tag,
            route: route,
        }
    }
}

impl<H: HyperTransport> HyperHandler<H> for Handler {
    fn on_request(&mut self, req: &mut Request) -> Next {
        debug!("{} send request {}", self.tag, req.uri());

        req.headers_mut().set(XRequestGuid(self.uuid.simple().to_string()));

        match self.route {
            Route::Msg { ref mut msg } => msg.on_request(req),
            _ => unimplemented!(),
        }
    }

    fn on_request_writable(&mut self, encoder: &mut Encoder<H>) -> Next {
        debug!("{} on request writable", self.tag);
        match self.route {
            Route::Msg { ref mut msg } => msg.on_request_writable(encoder),
            _ => unimplemented!(),
        }
    }

    fn on_response(&mut self, res: Response) -> Next {
        debug!("{} on response {:?}", self.tag, res);
        match self.route {
            Route::Msg { ref mut msg } => msg.on_response(res),
            _ => unimplemented!(),
        }
    }

    fn on_response_readable(&mut self, decoder: &mut Decoder<H>) -> Next {
        debug!("{} on response readable", self.tag);

        match self.route {
            Route::Msg { ref mut msg } => msg.on_response_readable(decoder),
            _ => unimplemented!(),
        }
    }

    fn on_error(&mut self, err: HyperError) -> Next {
        debug!("{} on error {:?}", self.tag, err);
        match self.route {
            Route::Msg { ref mut msg } => msg.on_error(err),
            _ => unimplemented!(),
        }
    }
}

pub struct Client {
    c: HyperClient<Handler>,
}

impl Client {
    pub fn new() -> Result<Client> {
        // TODO: support HTTPS later.
        let c = try!(HyperClient::<Handler>::configure()
                         .connector(HttpConnector::default())
                         .keep_alive(true)
                         .build());

        Ok(Client { c: c })
    }

    pub fn close(self) {
        self.c.close()
    }

    fn request(&self, url: Url, h: Handler) -> Result<()> {
        if let Err(e) = self.c.request(url, h) {
            return Err(box_err!("request failed {}", e));
        }
        Ok(())
    }

    pub fn post_message(&self, url: Url, msg: msgpb::Message, cb: OnResponse) -> Result<()> {
        let pm = Msg::new(Some(msg), box move |res| cb.call_box((res,)));

        let h = Handler::new(Route::Msg { msg: pm });
        self.request(url, h)
    }

    pub fn post_message_timeout(&self,
                                url: Url,
                                msg: msgpb::Message,
                                timeout: Duration)
                                -> Result<Option<msgpb::Message>> {
        let finished = Event::new();
        let finished2 = finished.clone();

        try!(self.post_message(url,
                               msg,
                               box move |res| {
                                   finished2.set(res);
                               }));

        if finished.wait_timeout(Some(timeout)) {
            return finished.take().unwrap();
        }

        Err(Error::Timeout(timeout))
    }
}

impl Clone for Client {
    fn clone(&self) -> Client {
        Client { c: self.c.clone() }
    }
}
