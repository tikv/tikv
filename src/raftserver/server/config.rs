const DEFAULT_LISTENING_ADDR: &'static str = "0.0.0.0:20160";
const DEFAULT_MAX_CONN_CAPACITY: usize = 4096;

pub struct Config {
    // Server listening address.
    pub addr: String,

    pub max_conn_capacity: usize,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            addr: DEFAULT_LISTENING_ADDR.to_owned(),

            max_conn_capacity: DEFAULT_MAX_CONN_CAPACITY,
        }
    }
}

impl Config {
    pub fn new() -> Config {
        Config::default()
    }
}
