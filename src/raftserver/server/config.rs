pub use raftserver::store::Config as StoreConfig;
use raftserver::Result;

const DEFAULT_CLUSTER_ID: u64 = 0;
const DEFAULT_LISTENING_ADDR: &'static str = "127.0.0.1:20160";
const DEFAULT_ADVERTISE_LISTENING_ADDR: &'static str = "127.0.0.1:20160";
const DEFAULT_MAX_CONN_CAPACITY: usize = 4096;
const DEFAULT_ADVERTISE_CLIENT_ADDR: &'static str = "127.0.0.1:6102";

#[derive(Clone, Debug)]
pub struct Config {
    pub cluster_id: u64,

    // Raft Server listening address.
    pub addr: String,

    // Raft Server advertise listening address for outer communication.
    pub advertise_addr: String,

    // Advertise address for communication with node and client.
    // This field should not be here, but the node meta needs it.
    // TODO: we should combine raft addr and client addr together later.
    pub advertise_client_addr: String,

    pub max_conn_capacity: usize,

    pub store_cfg: StoreConfig,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            cluster_id: DEFAULT_CLUSTER_ID,
            addr: DEFAULT_LISTENING_ADDR.to_owned(),
            advertise_addr: DEFAULT_ADVERTISE_LISTENING_ADDR.to_owned(),
            advertise_client_addr: DEFAULT_ADVERTISE_CLIENT_ADDR.to_owned(),
            max_conn_capacity: DEFAULT_MAX_CONN_CAPACITY,
            store_cfg: StoreConfig::default(),
        }
    }
}

impl Config {
    pub fn new() -> Config {
        Config::default()
    }

    pub fn validate(&self) -> Result<()> {
        try!(self.store_cfg.validate());

        Ok(())
    }
}
