mod conn;
mod server;

pub mod run;

use storage;

pub use self::run::run;

quick_error! {
    #[derive(Debug)]
    pub enum ServerError {
        Storage(err: storage::Error) {
            from()
            cause(err)
            description(err.description())
        }
        FormatError(desc: String) {
            description(desc)
        }
    }
}

pub type Result<T> = ::std::result::Result<T, ServerError>;
