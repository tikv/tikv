mod scheduler;

pub use self::scheduler::Scheduler;

use storage::mvcc;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Mvcc(err: mvcc::Error) {
            from()
            cause(err)
            description(err.description())
        }
        TxnNotFound {description("txn not found")}
    }
}

pub type Result<T> = ::std::result::Result<T, Error>;
