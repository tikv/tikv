use protobuf;
use uuid::Uuid;
use kvproto::{metapb, pdpb};
use super::{Error, Result, errors, TRpcClient, Client};

impl<T: TRpcClient> super::PdClient for Client<T> {
    fn bootstrap_cluster(&mut self,
                         cluster_id: u64,
                         node: metapb::Node,
                         stores: Vec<metapb::Store>,
                         region: metapb::Region)
                         -> Result<()> {
        let mut bootstrap = pdpb::BootstrapRequest::new();
        bootstrap.set_node(node);
        bootstrap.set_stores(protobuf::RepeatedField::from_vec(stores));
        bootstrap.set_region(region);

        let mut req = new_request(cluster_id, pdpb::CommandType::Bootstrap);
        req.set_bootstrap(bootstrap);

        let resp = try!(self.send_message(&req));
        try!(check_resp(&resp));
        Ok(())
    }

    fn is_cluster_bootstrapped(&self, cluster_id: u64) -> Result<bool> {
        let mut req = new_request(cluster_id, pdpb::CommandType::IsBootstrapped);
        req.set_is_bootstrapped(pdpb::IsBootstrappedRequest::new());

        let resp = try!(self.send_message(&req));
        try!(check_resp(&resp));
        Ok(resp.get_is_bootstrapped().get_bootstrapped())
    }

    fn alloc_id(&mut self) -> Result<u64> {
        let mut req = new_request(0, pdpb::CommandType::AllocId);
        req.set_alloc_id(pdpb::AllocIdRequest::new());

        let resp = try!(self.send_message(&req));
        try!(check_resp(&resp));
        Ok(resp.get_alloc_id().get_id())
    }

    fn put_node(&mut self, cluster_id: u64, node: metapb::Node) -> Result<()> {
        let mut put_meta = pdpb::PutMetaRequest::new();
        put_meta.set_meta_type(pdpb::MetaType::NodeType);
        put_meta.set_node(node);

        let mut req = new_request(cluster_id, pdpb::CommandType::PutMeta);
        req.set_put_meta(put_meta);

        let resp = try!(self.send_message(&req));
        try!(check_resp(&resp));
        Ok(())
    }

    fn put_store(&mut self, cluster_id: u64, store: metapb::Store) -> Result<()> {
        let mut put_meta = pdpb::PutMetaRequest::new();
        put_meta.set_meta_type(pdpb::MetaType::StoreType);
        put_meta.set_store(store);

        let mut req = new_request(cluster_id, pdpb::CommandType::PutMeta);
        req.set_put_meta(put_meta);

        let resp = try!(self.send_message(&req));
        try!(check_resp(&resp));
        Ok(())
    }

    fn delete_node(&mut self, cluster_id: u64, node_id: u64) -> Result<()> {
        let mut delete_meta = pdpb::DeleteMetaRequest::new();
        delete_meta.set_meta_type(pdpb::MetaType::NodeType);
        delete_meta.set_node_id(node_id);

        let mut req = new_request(cluster_id, pdpb::CommandType::DeleteMeta);
        req.set_delete_meta(delete_meta);

        let resp = try!(self.send_message(&req));
        try!(check_resp(&resp));
        Ok(())
    }

    fn delete_store(&mut self, cluster_id: u64, store_id: u64) -> Result<()> {
        let mut delete_meta = pdpb::DeleteMetaRequest::new();
        delete_meta.set_meta_type(pdpb::MetaType::StoreType);
        delete_meta.set_store_id(store_id);

        let mut req = new_request(cluster_id, pdpb::CommandType::DeleteMeta);
        req.set_delete_meta(delete_meta);

        let resp = try!(self.send_message(&req));
        try!(check_resp(&resp));
        Ok(())
    }

    fn get_node(&self, cluster_id: u64, node_id: u64) -> Result<metapb::Node> {
        let mut get_meta = pdpb::GetMetaRequest::new();
        get_meta.set_meta_type(pdpb::MetaType::NodeType);
        get_meta.set_node_id(node_id);

        let mut req = new_request(cluster_id, pdpb::CommandType::GetMeta);
        req.set_get_meta(get_meta);

        let resp = try!(self.send_message(&req));
        try!(check_resp(&resp));
        Ok(resp.get_get_meta().get_node().clone())
    }

    fn get_store(&self, cluster_id: u64, store_id: u64) -> Result<metapb::Store> {
        let mut get_meta = pdpb::GetMetaRequest::new();
        get_meta.set_meta_type(pdpb::MetaType::StoreType);
        get_meta.set_store_id(store_id);

        let mut req = new_request(cluster_id, pdpb::CommandType::GetMeta);
        req.set_get_meta(get_meta);

        let resp = try!(self.send_message(&req));
        try!(check_resp(&resp));
        Ok(resp.get_get_meta().get_store().clone())
    }

    fn get_cluster_meta(&self, cluster_id: u64) -> Result<metapb::Cluster> {
        let mut get_meta = pdpb::GetMetaRequest::new();
        get_meta.set_meta_type(pdpb::MetaType::ClusterType);
        get_meta.set_cluster_id(cluster_id);

        let mut req = new_request(cluster_id, pdpb::CommandType::GetMeta);
        req.set_get_meta(get_meta);

        let resp = try!(self.send_message(&req));
        try!(check_resp(&resp));
        Ok(resp.get_get_meta().get_cluster().clone())
    }

    fn get_region(&self, cluster_id: u64, key: &[u8]) -> Result<metapb::Region> {
        let mut get_meta = pdpb::GetMetaRequest::new();
        get_meta.set_meta_type(pdpb::MetaType::RegionType);
        get_meta.set_region_key(key.to_vec());

        let mut req = new_request(cluster_id, pdpb::CommandType::GetMeta);
        req.set_get_meta(get_meta);

        let resp = try!(self.send_message(&req));
        try!(check_resp(&resp));
        Ok(resp.get_get_meta().get_region().clone())
    }

    fn ask_change_peer(&self,
                       cluster_id: u64,
                       region: metapb::Region,
                       leader: metapb::Peer)
                       -> Result<()> {
        let mut ask_change_peer = pdpb::AskChangePeerRequest::new();
        ask_change_peer.set_region(region);
        ask_change_peer.set_leader(leader);

        let mut req = new_request(cluster_id, pdpb::CommandType::AskChangePeer);
        req.set_ask_change_peer(ask_change_peer);

        self.post_message::<_, pdpb::Response>(req)
    }

    fn ask_split(&self,
                 cluster_id: u64,
                 region: metapb::Region,
                 split_key: &[u8],
                 leader: metapb::Peer)
                 -> Result<()> {
        let mut ask_split = pdpb::AskSplitRequest::new();
        ask_split.set_region(region);
        ask_split.set_split_key(split_key.to_vec());
        ask_split.set_leader(leader);

        let mut req = new_request(cluster_id, pdpb::CommandType::AskSplit);
        req.set_ask_split(ask_split);

        self.post_message::<_, pdpb::Response>(req)
    }
}

fn new_request(cluster_id: u64, cmd_type: pdpb::CommandType) -> pdpb::Request {
    let mut header = pdpb::RequestHeader::new();
    header.set_cluster_id(cluster_id);
    header.set_uuid(Uuid::new_v4().as_bytes().to_vec());
    let mut req = pdpb::Request::new();
    req.set_header(header);
    req.set_cmd_type(cmd_type);
    req
}

fn check_resp(resp: &pdpb::Response) -> Result<()> {
    if !resp.has_header() {
        return Err(errors::other("pd response missing header"));
    }
    let header = resp.get_header();
    if !header.has_error() {
        return Ok(());
    }
    let error = header.get_error();
    // TODO: translate more error types
    if error.has_bootstrapped() {
        Err(Error::ClusterBootstrapped(header.get_cluster_id()))
    } else {
        Err(errors::other(error.get_message()))
    }
}
