use std::boxed::Box;
use std::error::Error;
use proto::kvrpc::{GetRequest, GetResponse, Request, Response, MessageType};
use kvserver::server::Dispatcher;
use protobuf;
use protobuf::ProtobufEnum;

pub struct KVDispatch;

impl KVDispatch {
    pub fn new() -> KVDispatch {
        KVDispatch
    }
    pub fn handle_get(&mut self, msg: &Request) -> Result<Response, Box<Error + Send + Sync>> {
        assert!(msg.has_getReq());
        let get_req: &GetRequest = msg.get_getReq();
        let mut get_resp: GetResponse = GetResponse::new();
        // [TODO]: construct get response
        let mut resp: Response = Response::new();
        resp.set_getResp(get_resp);
        Ok(resp)
    }
}
impl Dispatcher for KVDispatch {
    fn Dispatch(&mut self, m: Request) -> Result<Response, Box<Error + Send + Sync>> {
        match m.get_field_type() {
            MessageType::Get => {
                match self.handle_get(&m) {
                    Err(why) => return Err(From::from(why)),
                    Ok(resp) => return Ok(resp),
                }
            }
        }
        Err(From::from("Message dismatch any type"))
    }
}
