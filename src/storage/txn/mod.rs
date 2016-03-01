mod shard_mutex;
mod store;
mod scheduler;

pub use self::scheduler::Scheduler;
pub use self::store::TxnStore;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Engine(err: ::storage::engine::Error) {
            from()
            cause(err)
            description(err.description())
        }
        Mvcc(err: ::storage::mvcc::Error) {
            from()
            cause(err)
            description(err.description())
        }
        TxnNotFound {description("txn not found")}
    }
}

pub type Result<T> = ::std::result::Result<T, Error>;
