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

use std::error;
use std::result;
use std::io::{Read, Write, Error as IoError};
use std::time::Duration;
use std::collections::BTreeMap;

use hyper::client::{Client, Handler, Request, Response, HttpConnector};
use hyper::{Decoder, Encoder, Next, Error as HyperError, Post, StatusCode};
use hyper::net::Transport;
use hyper::header::ContentLength;
use hyper::Url;
use bytes::{Buf, MutBuf, ByteBuf, MutByteBuf};
use rustc_serialize::json::{Json, ToJson};
use rustc_serialize::base64::{FromBase64, ToBase64, STANDARD};
use protobuf::Message;

use kvproto::pdpb::Leader;

use util::event::Event;
use util::buf::{TryRead, TryWrite, create_mem_buf};
use util::escape;

const HTTP_REQUEST_TIMEOUT: u64 = 3;
const ETCD_V3_KV_RANGE_PATH: &'static str = "/v3alpha/kv/range";

quick_error!{
    #[derive(Debug)]
    pub enum Error {
        Io(err: IoError) {
            from()
            cause(err)
            description(err.description())
        }
        Hyper(err: HyperError) {
            from()
            cause(err)
            description(err.description())
        }
        Status(status: StatusCode) {
            description("invalid HTTP status code")
            display("invalid HTTP status code {:?}", status)
        }
        Timeout(d: Duration) {
            description("request timeout")
            display("timeout after {:?}", d)
        }
        Other(err: Box<error::Error + Sync + Send>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("unknown error {:?}", err)
        }
    }
}

pub type Result<T> = result::Result<T, Error>;

fn next_read() -> Next {
    Next::read().timeout(Duration::from_secs(HTTP_REQUEST_TIMEOUT))
}

fn next_write() -> Next {
    Next::write().timeout(Duration::from_secs(HTTP_REQUEST_TIMEOUT))
}

// Encode the key with base64 format and then covert
// to Json object.
pub fn b64encode_to_json(key: &[u8]) -> Json {
    key.to_base64(STANDARD).to_json()
}

fn parse_response<T: Read>(r: &mut T) -> Result<Json> {
    let obj = box_try!(Json::from_reader(r));
    Ok(obj)
}

pub fn parse_leader_addr(obj: &Json, leader_key: &[u8]) -> Result<String> {
    if let Some(&Json::Array(ref kvs)) = obj.find("kvs") {
        if kvs.len() != 1 {
            return Err(box_err!("invalid response, kvs must have one item"));
        }

        if let Some(&Json::String(ref key)) = kvs[0].find("key") {
            // the key is base64 format, we must decode it.
            let key = box_try!(key.from_base64());
            if key != leader_key {
                return Err(box_err!("mismatch leader key, {:?} != {:?}",
                                    escape(&key),
                                    escape(leader_key)));
            }
        } else {
            return Err(box_err!("missing leader key"));
        }

        if let Some(&Json::String(ref value)) = kvs[0].find("value") {
            // the value is base64 format, we must decode it.
            let leader_value = box_try!(value.from_base64());

            let mut leader = Leader::new();
            box_try!(leader.merge_from_bytes(&leader_value));

            return Ok(leader.take_addr());
        }
    }

    Err(box_err!("fail to get pd leader"))
}

struct EtcdHandler {
    req_body: ByteBuf,
    resp_body: Option<MutByteBuf>,
    finish: Option<Event<Result<Json>>>,
}

impl EtcdHandler {
    pub fn new(req_body: Json, finish: Event<Result<Json>>) -> EtcdHandler {
        let data = format!("{}", req_body);
        let mut req_body = create_mem_buf(data.len());
        // must write ok.
        req_body.write(data.as_bytes()).unwrap();

        EtcdHandler {
            req_body: req_body.flip(),
            resp_body: None,
            finish: Some(finish),
        }
    }

    fn on_finish(&mut self, res: Result<Json>) -> Next {
        let next = if res.is_ok() {
            Next::end()
        } else {
            Next::remove()
        };

        if let Some(finish) = self.finish.take() {
            finish.set(res);
        }

        next
    }
}

impl Drop for EtcdHandler {
    fn drop(&mut self) {
        // Oh, something must go wrong, we forget finishing the request.
        if let Some(finish) = self.finish.take() {
            finish.set(Err(box_err!("forget to finish the request")));
        }
    }
}

impl<H: Transport> Handler<H> for EtcdHandler {
    fn on_request(&mut self, req: &mut Request) -> Next {
        req.set_method(Post);
        let content_len = Buf::bytes(&self.req_body).len() as u64;
        req.headers_mut().set(ContentLength(content_len));

        next_write()
    }

    fn on_request_writable(&mut self, encoder: &mut Encoder<H>) -> Next {
        if let Err(e) = encoder.try_write_buf(&mut self.req_body) {
            return self.on_finish(Err(Error::Io(e)));
        }

        if self.req_body.remaining() > 0 {
            // need to write more data
            return next_write();
        }

        next_read()
    }

    fn on_response(&mut self, res: Response) -> Next {
        let status = res.status();
        if *status != StatusCode::Ok {
            return self.on_finish(Err(Error::Status(*status)));
        }

        if let Some(len) = res.headers().get::<ContentLength>() {
            self.resp_body = Some(create_mem_buf(**len as usize));
            next_read()
        } else {
            // we must get ContentLength here
            return self.on_finish(Err(box_err!("response header missing Content-Length")));
        }
    }

    fn on_response_readable(&mut self, decoder: &mut Decoder<H>) -> Next {
        if self.resp_body.is_none() {
            return self.on_finish(
                Err(box_err!("request has been finished, can't read more response")));
        }

        let mut body = self.resp_body.take().unwrap();

        if let Err(e) = decoder.try_read_buf(&mut body) {
            return self.on_finish(Err(Error::Io(e)));
        }

        if body.remaining() > 0 {
            self.resp_body = Some(body);
            return next_read();
        }

        let mut body = body.flip();
        self.on_finish(parse_response(&mut body))
    }

    fn on_error(&mut self, err: HyperError) -> Next {
        self.on_finish(Err(Error::Hyper(err)))
    }
}

pub struct EtcdClient {
    root_path: String,
    endpoints: Vec<String>,
    next_index: usize,

    client: Client<EtcdHandler>,
}

impl EtcdClient {
    // pd_root is pd root in etcd, like /pd.
    // endpoints is etcd endpoints, format is 127.0.0.1:2379,127.0.0.1:3379.
    pub fn new(cluster_id: u64, pd_root: &str, endpoints: &str) -> Result<EtcdClient> {
        // only 1 thread is enough for pd now.
        // TODO: detect HTTP or HTTPs with SSL config.
        let endpoints: Vec<_> = endpoints.split(',').map(|v| format!("http://{}", v)).collect();
        assert!(!endpoints.is_empty());

        let client = box_try!(Client::<EtcdHandler>::configure()
            .connector(HttpConnector::default().threads(1))
            .keep_alive(true)
            .build());

        Ok(EtcdClient {
            root_path: format!("{}/{}", pd_root, cluster_id),
            endpoints: endpoints,
            next_index: 0,
            client: client,
        })
    }

    pub fn root_path(&self) -> &str {
        &self.root_path
    }

    pub fn request(&mut self, path: &str, obj: Json) -> Result<Json> {
        let endpoint = self.endpoints.get(self.next_index).unwrap();

        let finish = Event::new();
        let url = box_try!(Url::parse(&format!("{}{}", endpoint, path)));

        let handler = EtcdHandler::new(obj, finish.clone());
        if let Err(e) = self.client.request(url, handler) {
            return Err(box_err!("request {} failed {}", path, e));
        }

        let timeout = Duration::from_secs(HTTP_REQUEST_TIMEOUT);
        if !finish.wait_timeout(Some(timeout)) {
            return Err(Error::Timeout(timeout));
        }

        let res = finish.take().unwrap();
        if let Err(Error::Hyper(_)) = res {
            // we meet HyperError, maybe we can't communicate with
            // the etcd server, so try to next one next time.
            self.next_index = (self.next_index + 1) % self.endpoints.len();
        }

        res
    }

    pub fn get_leader_addr(&mut self) -> Result<String> {
        let mut obj = BTreeMap::new();
        let key = format!("{}/leader", self.root_path);
        obj.insert("key".to_owned(), b64encode_to_json(key.as_bytes()));

        let res = try!(self.request(ETCD_V3_KV_RANGE_PATH, Json::Object(obj)));
        parse_leader_addr(&res, key.as_bytes())
    }
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::collections::BTreeMap;

    use rustc_serialize::json::Json;
    use protobuf::Message;

    use kvproto::pdpb::Leader;
    use super::*;

    #[test]
    fn test_parse_leader() {
        let leader_key = b"/pd/0/leader";
        let leader_addr: &str = "127.0.0.1:1234";

        let resp = "{\"kvs\":[{\"key\":\"L3BkLzAvbGVhZGVy\",\"value\":\
                    \"Cg4xMjcuMC4wLjE6MTIzNBDheA==\"}]}";

        let obj = Json::from_str(resp).unwrap();
        let addr = parse_leader_addr(&obj, leader_key).unwrap();
        assert_eq!(addr, leader_addr);

        // test parse fail
        let tbls = vec![
            "{}",
            "{\"kvs\":[]}",
            "{\"kvs\":[{\"value\":\"Cg4xMjcuMC4wLjE6MTIzNBDheA==\"}]}",
            "{\"kvs\":[{\"key\":\"L3BkLzAvbGVhZGVy\"}]}",
            "{\"kvs\":[{\"key\":\"L3BkLzAvbGZGVy\",\"value\":\"Cg4xMjcuMC4wLjE6MTIzNBDheA==\"}]}",
            "{\"kvs\":[{\"key\":\"L3BkLzAvbGVhZGVy\",\"value\":\"Cg4xMjc4wLjE6MTIzNB==\"}]}",
            "{\"kvs\":[{\"key\":\"L3BkLzEvbGVhZGVy\",\"value\":\"Cg4xMjcuMC4wLjE6MTIzNBDheA==\"}]}",
            "{\"kvs\":[{\"key\":\"L3BkLzAvbGVhZGVy\",\"value\":\"L3BkLzEvbGVhZGVy\"}]}",
        ];

        for resp in tbls {
            let obj = Json::from_str(resp).unwrap();
            assert!(parse_leader_addr(&obj, leader_key).is_err(), resp);
        }
    }

    #[test]
    fn test_get_leader() {
        // If no ETCD_ENDPOINTS, skip this test.
        let endpoints = match env::var("ETCD_ENDPOINTS") {
            Err(_) => return,
            Ok(v) => v,
        };

        let mut client = EtcdClient::new(0, "/pd_test", &endpoints).unwrap();

        let key = format!("{}/leader", client.root_path());

        let mut leader = Leader::new();
        leader.set_addr("127.0.0.1:1234".to_owned());

        let value = leader.write_to_bytes().unwrap();
        let mut obj = BTreeMap::new();
        obj.insert("key".to_owned(), b64encode_to_json(key.as_bytes()));
        obj.insert("value".to_owned(), b64encode_to_json(&value));

        client.request("/v3alpha/kv/put", Json::Object(obj)).unwrap();

        let addr = client.get_leader_addr().unwrap();
        assert_eq!(&addr, leader.get_addr());

    }
}
