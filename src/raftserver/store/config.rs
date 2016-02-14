pub const DEFAULT_RAFT_BASE_TICK_INTERVAL: u64 = 100;
pub const DEFAULT_RAFT_HEARTBEAT_TICKS: usize = 3;
pub const DEFAULT_RAFT_ELECTION_TIMEOUT_TICKS: usize = 15;
pub const DEFAULT_RAFT_MAX_SIZE_PER_MSG: u64 = 1024 * 1024;
pub const DEFAULT_RAFT_MAX_INFLIGHT_MSGS: usize = 256;

#[derive(Debug, Clone)]
pub struct Config {
    // raft_base_tick_interval is a base tick interval (ms).
    pub raft_base_tick_interval: u64,
    pub raft_heartbeat_ticks: usize,
    pub raft_election_timeout_ticks: usize,
    pub raft_max_size_per_msg: u64,
    pub raft_max_inflight_msgs: usize,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            raft_base_tick_interval: DEFAULT_RAFT_BASE_TICK_INTERVAL,
            raft_heartbeat_ticks: DEFAULT_RAFT_HEARTBEAT_TICKS,
            raft_election_timeout_ticks: DEFAULT_RAFT_ELECTION_TIMEOUT_TICKS,
            raft_max_size_per_msg: DEFAULT_RAFT_MAX_SIZE_PER_MSG,
            raft_max_inflight_msgs: DEFAULT_RAFT_MAX_INFLIGHT_MSGS,
        }
    }
}

impl Config {
    pub fn new() -> Config {
        Config::default()
    }
}
