use raft::storage::Storage;


/// Raft log implementation
pub struct raft_log {
    store: Storage,
}
