use std::collections::VecDeque;
use super::mvcc::{self, MvccEngine};
use super::{Engine, Command, Key, Value, KvPair, Callback};

#[derive(Debug)]
#[allow(dead_code)]
enum Pending {
    Command(Command),
    WaitCommit {
        puts: Vec<KvPair>,
        deletes: Vec<Key>,
        start_version: u64,
    },
}

pub struct Scheduler {
    engine: Box<Engine>,
    pendings: VecDeque<Pending>,
}

impl Scheduler {
    pub fn new(engine: Box<Engine>) -> Scheduler {
        Scheduler {
            engine: engine,
            pendings: VecDeque::new(),
        }
    }

    pub fn handle_cmd(&mut self, cmd: Command) {
        match cmd {
            Command::Commit{commit_version, callback, ..} => {
                match self.pendings.pop_front() {
                    // TODO(disksing): check start_version
                    Some(Pending::WaitCommit{puts, deletes, ..}) => {
                        self.exec_commit(puts, deletes, commit_version, callback)
                    }
                    _ => unreachable!(), // TODO(disksing): return specific errors
                }
            }
            _ => self.pendings.push_back(Pending::Command(cmd)),
        }

        loop {
            match self.pendings.front() {
                Some(&Pending::WaitCommit{..}) | None => return,
                _ => {}
            }

            let front = self.pendings.pop_front().unwrap();
            match front {
                Pending::Command(Command::Get{key, version, callback}) => {
                    self.exec_get(key, version, callback)
                }
                Pending::Command(Command::Scan{start_key, limit, version, callback}) => {
                    self.exec_scan(start_key, limit, version, callback)
                }
                Pending::Command(Command::Prewrite{puts, deletes, locks, start_version, callback}) => {
                    self.exec_prewrite(puts, deletes, locks, start_version, callback)
                }
                _ => unreachable!(),
            }
        }
    }

    fn exec_get(&self, key: Key, version: u64, callback: Callback<Option<Value>>) {
        let value = self.engine.mvcc_get(&key, version);
        callback(value.map_err(super::Error::from));
    }

    fn exec_scan(&self,
                 start_key: Key,
                 limit: usize,
                 version: u64,
                 callback: Callback<Vec<KvPair>>) {
        let pairs = self.engine.mvcc_scan(&start_key, limit, version);
        callback(pairs.map_err(super::Error::from));
    }

    fn exec_prewrite(&mut self,
                     puts: Vec<KvPair>,
                     deletes: Vec<Key>,
                     locks: Vec<Key>,
                     start_version: u64,
                     callback: Callback<()>) {
        match self.check_prewrite(&puts, &deletes, &locks, start_version) {
            Ok(_) => {
                self.pendings.push_front(Pending::WaitCommit {
                    puts: puts,
                    deletes: deletes,
                    start_version: start_version,
                });
                callback(Ok(()));
            }
            Err(e) => callback(Err(super::Error::from(e))),
        }
    }

    fn check_prewrite(&self,
                      puts: &[KvPair],
                      deletes: &[Key],
                      locks: &[Key],
                      start_version: u64)
                      -> Result<()> {
        for key in puts.iter().map(|&(ref x, _)| x).chain(deletes.iter()).chain(locks.iter()) {
            let latest_version = try!(self.engine.mvcc_latest_modified(key));
            if let Some(ver) = latest_version {
                if ver >= start_version {
                    return Err(Error::ConditionNotMatch);
                }
            }
        }
        Ok(())
    }

    fn exec_commit(&mut self,
                   puts: Vec<KvPair>,
                   deletes: Vec<Key>,
                   commit_version: u64,
                   callback: Callback<()>) {
        callback(self.try_commit(puts, deletes, commit_version).map_err(super::Error::from));
    }

    fn try_commit(&mut self,
                  puts: Vec<KvPair>,
                  deletes: Vec<Key>,
                  commit_version: u64)
                  -> Result<()> {
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
