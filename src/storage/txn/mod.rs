use super::kv::Command;
use super::mvcc;

pub struct Scheduler;

impl Scheduler {
    pub fn new() -> Scheduler {
        Scheduler
    }

    pub fn handle_cmd(&mut self, cmd: Command) {
        match cmd {
            Command::Get((_, callback)) => callback(Ok(None)),
            Command::Scan((_, callback)) => callback(Ok(vec![])),
            Command::Commit((_, callback)) => callback(Ok(())),
        }
    }
}

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Mvcc(err: mvcc::Error) {
            from()
            cause(err)
            description(err.description())
        }
        ConditionNotMatch {description("condition not match")}
    }
}

pub type Result<T> = ::std::result::Result<T, Error>;
