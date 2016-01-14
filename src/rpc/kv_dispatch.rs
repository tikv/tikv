use std::boxed::Box;
use std::error::Error;
use proto::kvrpc::{FooRequest, FooResponse, Message};
use kvserver::server::Dispatcher;
use protobuf;
use protobuf::ProtobufEnum;

pub struct KVDispatch;

impl KVDispatch {
    pub fn new() -> KVDispatch {
        KVDispatch
    }
    pub fn handle_foo(&mut self, msg: &Message) -> Result<Message, Box<Error + Send + Sync>> {
        assert!(msg.has_fooReq());
        let foo_req: &FooRequest = msg.get_fooReq();
        let mut foo_resp: FooResponse = FooResponse::new();
        // [TODO]: construct foo response
        let mut resp: Message = Message::new();
        resp.set_fooResp(foo_resp);
        Ok(resp)
    }
}
impl Dispatcher for KVDispatch {
    fn Dispatch(&mut self, m: Message) -> Result<Message, Box<Error + Send + Sync>> {
        if m.has_fooReq() {
            match self.handle_foo(&m) {
                Err(why) => return Err(From::from(why)),
                Ok(resp) => return Ok(resp),
            }
        }
        Err(From::from("Message dismatch any type"))
    }
}
