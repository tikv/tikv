use std::result;
use std::error;
use std::time::Duration;

// TODO: It's only a wrapper for Coprocessor Error. The dag need a better Error.
quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Eval(err: tipb::select::Error) {
            from()
            description("eval failed")
            display("Eval error: {}", err.get_msg())
        }
        Outdated(elapsed: Duration, tag: &'static str) {
            description("request is outdated")
        }
        Other(err: Box<dyn error::Error + Send + Sync>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("{}", err)
        }
    }
}

pub type Result<T> = result::Result<T, Error>;