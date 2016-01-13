
#![allow(dead_code)]
use raft::storage::Storage;


/// Raft log implementation
pub struct RaftLog<T: Storage> {
    store: T,
}
