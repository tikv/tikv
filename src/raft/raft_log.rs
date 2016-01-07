use raft::storage::Storage;


/// Raft log implementation
pub struct RaftLog {
    store: Storage,
}
