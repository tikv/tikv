use std::error;
use std::boxed::Box;
use std::result;

quick_error!{
    #[derive(Debug)]
    pub enum Error {
        ClusterBootstrapped(cluster_id: u64) {
            description("cluster bootstrap error")
            display("cluster {} is already bootstrapped", cluster_id)
        }
        ClusterNotBootstrapped(cluster_id: u64) {
            description("cluster not bootstrap error")
            display("cluster {} is not bootstrapped", cluster_id)
        }
        DeleteNotEmptyNode(node_id: u64) {
            description("delete not empty node")
            display("node {} is not empty, can not be deleted", node_id)
        }
        DeleteNotEmptyStore(store_id: u64) {
            description("delete not empty store")
            display("store {} is not empty, can not be deleted", store_id)
        }
        Other(err: Box<error::Error + Sync + Send>) {
            cause(err.as_ref())
            description(err.description())
        }
    }
}


pub type Result<T> = result::Result<T, Error>;

pub fn other<T>(err: T) -> Error
    where T: Into<Box<error::Error + Sync + Send + 'static>>
{
    Error::Other(err.into())
}
