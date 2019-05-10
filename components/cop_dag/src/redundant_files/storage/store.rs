// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::storage::types::Key;
use crate::storage::Statistics;
use crate::storage::{KvPair, Value};

use std::error;
use std::io::Error as IoError;
use tikv_util::escape;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        // TODO: Add error for Engine
//        Engine(err: crate::storage::kv::Error) {
//            from()
//            cause(err)
//            description(err.description())
//        }
        Codec(err: tikv_util::codec::Error) {
            from()
            cause(err)
            description(err.description())
        }
        ProtoBuf(err: protobuf::error::ProtobufError) {
            from()
            cause(err)
            description(err.description())
        }
        // TODO: Add error for mvcc
//        Mvcc(err: crate::storage::mvcc::Error) {
//            from()
//            cause(err)
//            description(err.description())
//        }
        Other(err: Box<dyn error::Error + Sync + Send>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("{:?}", err)
        }
        Io(err: IoError) {
            from()
            cause(err)
            description(err.description())
        }
        InvalidTxnTso {start_ts: u64, commit_ts: u64} {
            description("Invalid transaction tso")
            display("Invalid transaction tso with start_ts:{},commit_ts:{}",
                        start_ts,
                        commit_ts)
        }
        InvalidReqRange {start: Option<Vec<u8>>,
                        end: Option<Vec<u8>>,
                        lower_bound: Option<Vec<u8>>,
                        upper_bound: Option<Vec<u8>>} {
            description("Invalid request range")
            display("Request range exceeds bound, request range:[{:?}, end:{:?}), physical bound:[{:?}, {:?})",
                        start.as_ref().map(|s| escape(&s)),
                        end.as_ref().map(|e| escape(&e)),
                        lower_bound.as_ref().map(|s| escape(&s)),
                        upper_bound.as_ref().map(|s| escape(&s)))
        }
    }
}

impl Error {
    pub fn maybe_clone(&self) -> Option<Error> {
        match *self {
            //            Error::Engine(ref e) => e.maybe_clone().map(Error::Engine),
            Error::Codec(ref e) => e.maybe_clone().map(Error::Codec),
            //            Error::Mvcc(ref e) => e.maybe_clone().map(Error::Mvcc),
            Error::InvalidTxnTso {
                start_ts,
                commit_ts,
            } => Some(Error::InvalidTxnTso {
                start_ts,
                commit_ts,
            }),
            Error::InvalidReqRange {
                ref start,
                ref end,
                ref lower_bound,
                ref upper_bound,
            } => Some(Error::InvalidReqRange {
                start: start.clone(),
                end: end.clone(),
                lower_bound: lower_bound.clone(),
                upper_bound: upper_bound.clone(),
            }),
            Error::Other(_) | Error::ProtoBuf(_) | Error::Io(_) => None,
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

pub trait Store: Send {
    type Scanner: Scanner;

    fn get(&self, key: &Key, statistics: &mut Statistics) -> Result<Option<Value>>;

    fn batch_get(&self, keys: &[Key], statistics: &mut Statistics) -> Vec<Result<Option<Value>>>;

    fn scanner(
        &self,
        desc: bool,
        key_only: bool,
        lower_bound: Option<Key>,
        upper_bound: Option<Key>,
    ) -> Result<Self::Scanner>;
}

pub trait Scanner: Send {
    fn next(&mut self) -> Result<Option<(Key, Value)>>;

    fn scan(&mut self, limit: usize) -> Result<Vec<Result<KvPair>>> {
        let mut results = Vec::with_capacity(limit);
        while results.len() < limit {
            match self.next() {
                Ok(Some((k, v))) => {
                    results.push(Ok((k.to_raw()?, v)));
                }
                Ok(None) => break,
                // TODO: Add error for mvcc
                //                Err(e @ Error::Mvcc(MvccError::KeyIsLocked { .. })) => {
                //                    results.push(Err(e));
                //                }
                Err(e) => return Err(e),
            }
        }
        Ok(results)
    }

    fn take_statistics(&mut self) -> Statistics;
}

/// A Store that reads on fixtures.
pub struct FixtureStore {
    data: std::collections::BTreeMap<Key, Result<Vec<u8>>>,
}

impl Clone for FixtureStore {
    fn clone(&self) -> Self {
        let data = self
            .data
            .iter()
            .map(|(k, v)| {
                let owned_k = k.clone();
                let owned_v = match v {
                    Ok(v) => Ok(v.clone()),
                    Err(e) => Err(e.maybe_clone().unwrap()),
                };
                (owned_k, owned_v)
            })
            .collect();
        Self { data }
    }
}

impl FixtureStore {
    pub fn new(data: std::collections::BTreeMap<Key, Result<Vec<u8>>>) -> Self {
        FixtureStore { data }
    }
}

impl Store for FixtureStore {
    type Scanner = FixtureStoreScanner;

    #[inline]
    fn get(&self, key: &Key, _statistics: &mut Statistics) -> Result<Option<Vec<u8>>> {
        let r = self.data.get(key);
        match r {
            None => Ok(None),
            Some(Ok(v)) => Ok(Some(v.clone())),
            Some(Err(e)) => Err(e.maybe_clone().unwrap()),
        }
    }

    #[inline]
    fn batch_get(&self, keys: &[Key], statistics: &mut Statistics) -> Vec<Result<Option<Vec<u8>>>> {
        keys.iter().map(|key| self.get(key, statistics)).collect()
    }

    #[inline]
    fn scanner(
        &self,
        desc: bool,
        key_only: bool,
        lower_bound: Option<Key>,
        upper_bound: Option<Key>,
    ) -> Result<FixtureStoreScanner> {
        use std::ops::Bound;

        let lower = lower_bound.as_ref().map_or(Bound::Unbounded, |v| {
            if !desc {
                Bound::Included(v)
            } else {
                Bound::Excluded(v)
            }
        });
        let upper = upper_bound.as_ref().map_or(Bound::Unbounded, |v| {
            if desc {
                Bound::Included(v)
            } else {
                Bound::Excluded(v)
            }
        });

        let mut vec: Vec<_> = self
            .data
            .range((lower, upper))
            .map(|(k, v)| {
                let owned_k = k.clone();
                let owned_v = if key_only {
                    match v {
                        Ok(_v) => Ok(vec![]),
                        Err(e) => Err(e.maybe_clone().unwrap()),
                    }
                } else {
                    match v {
                        Ok(v) => Ok(v.clone()),
                        Err(e) => Err(e.maybe_clone().unwrap()),
                    }
                };
                (owned_k, owned_v)
            })
            .collect();

        if desc {
            vec.reverse();
        }

        Ok(FixtureStoreScanner {
            // TODO: Remove clone when GATs is available. See rust-lang/rfcs#1598.
            data: vec.into_iter(),
        })
    }
}

/// A Scanner that scans on fixtures.
pub struct FixtureStoreScanner {
    data: std::vec::IntoIter<(Key, Result<Vec<u8>>)>,
}

impl Scanner for FixtureStoreScanner {
    #[inline]
    fn next(&mut self) -> Result<Option<(Key, Vec<u8>)>> {
        let value = self.data.next();
        match value {
            None => Ok(None),
            Some((k, Ok(v))) => Ok(Some((k, v))),
            Some((_k, Err(e))) => Err(e),
        }
    }

    #[inline]
    fn take_statistics(&mut self) -> Statistics {
        Statistics::default()
    }
}
