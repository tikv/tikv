
#![allow(dead_code)]
use raft::storage::Storage;
use raft::log_unstable::Unstable;


/// Raft log implementation
pub struct RaftLog<T>
    where T: Storage
{
    // storage contains all stable entries since the last snapshot.
    store: T,

    // unstable contains all unstable entries and snapshot.
    // they will be saved into storage.
    unstable: Unstable,

    // committed is the highest log position that is known to be in
    // stable storage on a quorum of nodes.
    committed: u64,

    // applied is the highest log position that the application has
    // been instructed to apply to its state machine.
    // Invariant: applied <= committed
    applied: u64,
}

impl<T> RaftLog<T> where T: Storage
{
    pub fn new(storage: T) -> RaftLog<T> {
        let first_index = storage.first_index().unwrap_or_else(|e| panic!(e));
        let last_index = storage.last_index().unwrap_or_else(|e| panic!(e));

        let mut log = RaftLog {
            store: storage,
            committed: first_index - 1,
            applied: first_index - 1,
            unstable: Unstable::new(last_index + 1),
        };
        log
    }
}
