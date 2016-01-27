use super::mvcc::{self, MvccEngine};
use super::{Engine, Command, Key, KvPair};

pub struct Scheduler {
    engine: Box<Engine>,
}

impl Scheduler {
    pub fn new(engine: Box<Engine>) -> Scheduler {
        Scheduler { engine: engine }
    }

    pub fn handle_cmd(&mut self, cmd: Command) {
        match cmd {
            Command::Get{key, version, callback} => {
                let value = self.engine.mvcc_get(&key, version);
                callback(value.map_err(|e| super::Error::from(e)));
            }
            Command::Scan{start_key, limit, version, callback} => {
                let pairs = self.engine.mvcc_scan(&start_key, limit, version);
                callback(pairs.map_err(|e| super::Error::from(e)));
            }
            Command::Commit{puts, deletes, locks, start_version, commit_version, callback} => {
                callback(self.commit(puts, deletes, locks, start_version, commit_version)
                             .map_err(|e| super::Error::from(e)));
            }
        }
    }

    fn commit(&mut self,
              puts: Vec<KvPair>,
              deletes: Vec<Key>,
              locks: Vec<Key>,
              start_version: u64,
              commit_version: u64)
              -> Result<()> {
        for key in puts.iter().map(|&(ref x, _)| x).chain(deletes.iter()).chain(locks.iter()) {
            let latest_modify = try!(self.engine.as_ref().mvcc_latest_modify(key));
            if let Some(x) = latest_modify {
                if x >= start_version {
                    return Err(Error::ConditionNotMatch);
                }
            }
        }

        // TODO(disksing): use batch
        for (ref k, ref v) in puts {
            try!(self.engine.mvcc_put(k, v, commit_version));
        }
        for ref k in deletes {
            try!(self.engine.mvcc_delete(k, commit_version));
        }
        Ok(())
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
