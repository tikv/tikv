pub use raftserver::store::Config as StoreConfig;
use raftserver::Result;

const DEFAULT_CLUSTER_ID: u64 = 0;
const DEFAULT_LISTENING_ADDR: &'static str = "0.0.0.0:20160";
const DEFAULT_MAX_CONN_CAPACITY: usize = 4096;

#[derive(Clone, Debug)]
pub struct Config {
    pub cluster_id: u64,

    // Server listening address.
    pub addr: String,

    pub max_conn_capacity: usize,

    pub store_cfg: StoreConfig,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            cluster_id: DEFAULT_CLUSTER_ID,
            addr: DEFAULT_LISTENING_ADDR.to_owned(),
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
