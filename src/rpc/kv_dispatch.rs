use std::boxed::Box;
use std::error::Error;
use proto::kvrpcpb::{CmdGetRequest, CmdGetResponse, Request, Response, MessageType};
use kvserver::server::Dispatcher;
use protobuf;
use protobuf::ProtobufEnum;
use storage::Storage;

pub struct KVDispatch {
    store: Storage,
}

impl KVDispatch {
    pub fn new(store: Storage) -> KVDispatch {
        KVDispatch { store: store }
    }
    pub fn handle_get(&mut self, msg: &Request) -> Result<Response, Box<Error + Send + Sync>> {
        if !msg.has_cmd_get_req() {
            return Err(From::from("request body is missing"));
        }
        let cmd_get_req: &CmdGetRequest = msg.get_cmd_get_req();
        let mut cmd_get_resp: CmdGetResponse = CmdGetResponse::new();
        match self.store.get(cmd_get_req.get_key(), cmd_get_req.get_version()) {
            Ok(value) => {
                match valuc {
                    Some(v) => cmd_get_resp.set_value(v),
                    None => cmd_get_resp.set_ok(false),
                }
            }
            Err(why) => return Err(From::from(why)),
        }
        let mut resp: Response = Response::new();
        resp.set_cmd_get_resp(cmd_get_resp);
        Ok(resp)
    }
    pub fn handle_scan(&mut self, msg: &Request) -> Result<Response, Box<Error + Send + Sync>> {
        unimplemented!();
    }
    pub fn handle_put(&mut self, msg: &Request) -> Result<Response, Box<Error + Send + Sync>> {
        unimplemented!();
    }
    pub fn handle_del(&mut self, msg: &Request) -> Result<Response, Box<Error + Send + Sync>> {
        unimplemented!();
    }
    pub fn handle_commit(&mut self, msg: &Request) -> Result<Response, Box<Error + Send + Sync>> {
        unimplemented!();
    }
    pub fn handle_lock_keys(&mut self,
                            msg: &Request)
                            -> Result<Response, Box<Error + Send + Sync>> {
        unimplemented!();
    }
}
impl Dispatcher for KVDispatch {
    fn dispatch(&mut self, m: Request) -> Result<Response, Box<Error + Send + Sync>> {
        match m.get_field_type() {
            MessageType::CmdGet => {
                match self.handle_get(&m) {
                    Err(why) => return Err(From::from(why)),
                    Ok(resp) => return Ok(resp),
                }
            }
            MessageType::CmdScan => {
                match self.handle_scan(&m) {
                    Err(why) => return Err(From::from(why)),
                    Ok(resp) => return Ok(resp),
                }
            }
            MessageType::CmdPut => {
                match self.handle_put(&m) {
                    Err(why) => return Err(From::from(why)),
                    Ok(resp) => return Ok(resp),
                }
            }
            MessageType::CmdDel => {
                match self.handle_del(&m) {
                    Err(why) => return Err(From::from(why)),
                    Ok(resp) => return Ok(resp),
                }
            }
            MessageType::CmdCommit => {
                match self.handle_commit(&m) {
                    Err(why) => return Err(From::from(why)),
                    Ok(resp) => return Ok(resp),
                }
            }
            MessageType::CmdLockKeys => {
                match self.handle_lock_keys(&m) {
                    Err(why) => return Err(From::from(why)),
                    Ok(resp) => return Ok(resp),
                }
            }
        }
    }
}
