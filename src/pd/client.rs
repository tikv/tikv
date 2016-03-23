use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;
use std::sync::Mutex;
use util::codec::rpc;
use protobuf::{self, MessageStatic};
use super::{errors, Result};

pub trait TRpcClient: Sync + Send {
    fn send_message<M, P>(&self, msg_id: u64, message: &M) -> Result<P>
        where M: protobuf::Message,
              P: protobuf::Message + MessageStatic;
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
    fn send_message<M, P>(&self, msg_id: u64, message: &M) -> Result<P>
        where M: protobuf::Message,
              P: protobuf::Message + MessageStatic
    {
        // TODO: reconnect if error
        let mut stream = self.stream.lock().unwrap();
        try!(rpc::encode_msg(&mut *stream, msg_id, message));
        let mut resp = P::new();
        match try!(rpc::decode_msg(&mut *stream, &mut resp)) {
            id if id == msg_id => Ok(resp),
            _ => Err(errors::other("pd response msg_id not match")),
        }
    }
}

#[derive(Debug)]
pub struct Client<C: TRpcClient> {
    msg_id: Mutex<u64>,
    rpc_client: C,
}

impl<C: TRpcClient> Client<C> {
    pub fn new(rpc_client: C) -> Client<C> {
        Client {
            msg_id: Mutex::new(0),
            rpc_client: rpc_client,
        }
    }

    pub fn send_message<M, P>(&self, message: &M) -> Result<P>
        where M: protobuf::Message,
              P: protobuf::Message + MessageStatic
    {
        let id = self.alloc_msg_id();
        self.rpc_client.send_message(id, message)
    }

    // TODO: use a new thread to post message asynchronously.
    pub fn post_message<M, P>(&self, message: M) -> Result<()>
        where M: protobuf::Message,
              P: protobuf::Message + MessageStatic
    {
        let id = self.alloc_msg_id();
        try!(self.rpc_client.send_message::<_, P>(id, &message));
        Ok(())
    }

    fn alloc_msg_id(&self) -> u64 {
        let mut id = self.msg_id.lock().unwrap();
        *id += 1;
        *id
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
                    if meta.get_meta_type() == pdpb::MetaType::NodeType {
                        let node = meta.get_node();
                        assert_eq!(node.get_node_id(), 233);
                        assert_eq!(node.get_address(), "localhost");
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
        fn send_message<M, P>(&self, msg_id: u64, message: &M) -> Result<P>
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
    }

    #[test]
    fn test_pd_client() {
        let mut client = Client::new(MockRpcClient);
        assert_eq!(client.alloc_id().unwrap(), 42u64);
        assert!(client.is_cluster_bootstrapped(1).is_err());

        let mut node = metapb::Node::new();
        node.set_node_id(233);
        node.set_address("localhost".to_owned());
        assert!(client.put_node(1, node).is_ok());
    }
}
