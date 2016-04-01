use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;
use std::sync::{Arc, Mutex};
use std::sync::mpsc::{self, Sender};
use std::thread::{self, JoinHandle};
use util::codec::rpc;
use protobuf::{self, MessageStatic};
use super::Result;

pub trait TRpcClient: Sync + Send {
    fn send<M, P>(&self, msg_id: u64, message: &M) -> Result<P>
        where M: protobuf::Message,
              P: protobuf::Message + MessageStatic;
    fn post<M: protobuf::Message + ?Sized>(&self, msg_id: u64, message: &M) -> Result<()>;
}

#[derive(Debug)]
pub struct RpcClient {
    stream: Mutex<TcpStream>,
}

const SOCKET_READ_TIMEOUT: u64 = 3;
const SOCKET_WRITE_TIMEOUT: u64 = 3;

impl RpcClient {
    pub fn new<A: ToSocketAddrs>(addr: A) -> Result<RpcClient> {
        let stream = try!(TcpStream::connect(addr));
        try!(stream.set_nodelay(true));
        try!(stream.set_read_timeout(Some(Duration::from_secs(SOCKET_READ_TIMEOUT))));
        try!(stream.set_write_timeout(Some(Duration::from_secs(SOCKET_WRITE_TIMEOUT))));
        Ok(RpcClient { stream: Mutex::new(stream) })
    }
}

impl TRpcClient for RpcClient {
    fn send<M, P>(&self, msg_id: u64, message: &M) -> Result<P>
        where M: protobuf::Message,
              P: protobuf::Message + MessageStatic
    {
        // TODO: reconnect if error
        let mut stream = self.stream.lock().unwrap();
        try!(rpc::encode_msg(&mut *stream, msg_id, message));
        let mut resp = P::new();
        match try!(rpc::decode_msg(&mut *stream, &mut resp)) {
            id if id == msg_id => Ok(resp),
            _ => Err(box_err!("pd response msg_id not match")),
        }
    }

    fn post<M: protobuf::Message + ?Sized>(&self, msg_id: u64, message: &M) -> Result<()> {
        // TODO: reconnect if error
        let mut stream = self.stream.lock().unwrap();
        try!(rpc::encode_msg(&mut *stream, msg_id, message));
        match try!(rpc::decode_data(&mut *stream)) {
            (id, _) if id == msg_id => Ok(()),
            (id, _) => Err(box_err!("pd response msg_id not match, want {}, got {}", msg_id, id)),
        }
    }
}

#[derive(Debug)]
enum Msg {
    Rpc(u64, Box<protobuf::Message + Send>),
    Quit,
}

pub struct Client<C: TRpcClient + 'static> {
    msg_id: Mutex<u64>,
    rpc_client: Arc<C>,
    tx: Mutex<Sender<Msg>>,
    handle: Option<JoinHandle<()>>,
}

impl<C: TRpcClient + 'static> Client<C> {
    pub fn new(rpc_client: C) -> Result<Client<C>> {
        let shared = Arc::new(rpc_client);
        let (tx, rx) = mpsc::channel();
        let handle = {
            let rpc_client = shared.clone();
            try!(thread::Builder::new()
                     .name("pd-client background worker".to_owned())
                     .spawn(move || {
                         info!("pd-client background worker starting");
                         while let Msg::Rpc(msg_id, msg) = rx.recv().unwrap() {
                             if let Err(e) = rpc_client.post(msg_id, msg.as_ref()) {
                                 error!("pd post error: {}", e);
                             } else {
                                 debug!("pd post done: {}", msg_id);
                             }
                         }
                         info!("pd-client background worker closing");
                     }))
        };
        Ok(Client {
            msg_id: Mutex::new(0),
            rpc_client: shared.clone(),
            tx: Mutex::new(tx),
            handle: Some(handle),
        })
    }

    fn close(&mut self) {
        if let Err(e) = self.clone_tx().send(Msg::Quit) {
            error!("failed to send quit message to pd-client background worker: {}",
                   e);
        }
        let handle = self.handle.take().unwrap();
        if handle.join().is_err() {
            error!("failed to wait pd-client background worker quit");
        }
        info!("pd-client closed.");
    }

    pub fn send<M, P>(&self, message: &M) -> Result<P>
        where M: protobuf::Message,
              P: protobuf::Message + MessageStatic
    {
        let id = self.alloc_msg_id();
        self.rpc_client.send(id, message)
    }

    pub fn post<M: protobuf::Message + Send>(&self, message: M) -> Result<()> {
        let id = self.alloc_msg_id();
        if let Err(e) = self.clone_tx().send(Msg::Rpc(id, box message)) {
            return Err(box_err!(format!("SendError: {:?}", e)));
        }
        Ok(())
    }

    fn alloc_msg_id(&self) -> u64 {
        let mut id = self.msg_id.lock().unwrap();
        *id += 1;
        *id
    }

    fn clone_tx(&self) -> Sender<Msg> {
        let tx = self.tx.lock().unwrap();
        tx.clone()
    }
}

impl<C: TRpcClient + 'static> Drop for Client<C> {
    fn drop(&mut self) {
        self.close()
    }
}

#[cfg(test)]
mod tests {
    use super::super::{Result, PdClient};
    use super::*;
    use protobuf;
    use bytes::ByteBuf;
    use util::codec::rpc;
    use kvproto::{metapb, pdpb};

    struct MockRpcClient;

    impl MockRpcClient {
        fn reply(&self, req: &pdpb::Request) -> pdpb::Response {
            assert!(req.has_header());
            let header = req.get_header();
            let (uuid, cluster_id) = (header.get_uuid(), header.get_cluster_id());

            let mut resp = pdpb::Response::new();
            let mut header = pdpb::ResponseHeader::new();
            header.set_uuid(uuid.to_vec());
            header.set_cluster_id(cluster_id);

            match req.get_cmd_type() {
                pdpb::CommandType::AllocId => {
                    let mut alloc = pdpb::AllocIdResponse::new();
                    alloc.set_id(42);
                    resp.set_alloc_id(alloc);
                }
                pdpb::CommandType::PutMeta => {
                    let meta = req.get_put_meta();
                    if meta.get_meta_type() == pdpb::MetaType::StoreType {
                        let store = meta.get_store();
                        assert_eq!(store.get_id(), 233);
                        assert_eq!(store.get_address(), "localhost");
                    }
                }
                _ => {
                    let mut err = pdpb::Error::new();
                    err.set_message(String::from("I've got a bad feeling about this"));
                    header.set_error(err);
                }
            }

            resp.set_header(header);
            resp.set_cmd_type(req.get_cmd_type());
            resp
        }
    }

    impl TRpcClient for MockRpcClient {
        fn send<M, P>(&self, msg_id: u64, message: &M) -> Result<P>
            where M: protobuf::Message,
                  P: protobuf::Message + Default
        {
            let mut buf = ByteBuf::mut_with_capacity(128);
            rpc::encode_msg(&mut buf, msg_id, message).unwrap();
            let mut req = pdpb::Request::new();
            rpc::decode_msg(&mut buf.flip(), &mut req).unwrap();

            let resp = self.reply(&req);
            let mut buf = ByteBuf::mut_with_capacity(128);
            rpc::encode_msg(&mut buf, msg_id, &resp).unwrap();
            let mut msg = P::default();
            rpc::decode_msg(&mut buf.flip(), &mut msg).unwrap();
            Ok(msg)
        }

        fn post<M: protobuf::Message + ?Sized>(&self, _: u64, _: &M) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn test_pd_client() {
        let mut client = Client::new(MockRpcClient).unwrap();
        assert_eq!(client.alloc_id().unwrap(), 42u64);
        assert!(client.is_cluster_bootstrapped(1).is_err());

        let mut store = metapb::Store::new();
        store.set_id(233);
        store.set_address("localhost".to_owned());
        assert!(client.put_store(1, store).is_ok());
    }
}
