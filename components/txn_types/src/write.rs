// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use codec::prelude::NumberDecoder;
use std::mem::size_of;
use tikv_util::codec::number::{self, NumberEncoder, MAX_VAR_U64_LEN};

use crate::lock::LockType;
use crate::timestamp::TimeStamp;
use crate::types::{Value, SHORT_VALUE_PREFIX};
use crate::{Error, ErrorInner, Result};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum WriteType {
    Put,
    Delete,
    Lock,
    Rollback,
}

const FLAG_PUT: u8 = b'P';
const FLAG_DELETE: u8 = b'D';
const FLAG_LOCK: u8 = b'L';
const FLAG_ROLLBACK: u8 = b'R';

const FLAG_OVERLAPPED_ROLLBACK: u8 = b'R';

const GC_FENCE_PREFIX: u8 = b'F';

/// The short value for rollback records which are protected from being collapsed.
const PROTECTED_ROLLBACK_SHORT_VALUE: &[u8] = b"p";

impl WriteType {
    pub fn from_lock_type(tp: LockType) -> Option<WriteType> {
        match tp {
            LockType::Put => Some(WriteType::Put),
            LockType::Delete => Some(WriteType::Delete),
            LockType::Lock => Some(WriteType::Lock),
            LockType::Pessimistic => None,
        }
    }

    pub fn from_u8(b: u8) -> Option<WriteType> {
        match b {
            FLAG_PUT => Some(WriteType::Put),
            FLAG_DELETE => Some(WriteType::Delete),
            FLAG_LOCK => Some(WriteType::Lock),
            FLAG_ROLLBACK => Some(WriteType::Rollback),
            _ => None,
        }
    }

    fn to_u8(self) -> u8 {
        match self {
            WriteType::Put => FLAG_PUT,
            WriteType::Delete => FLAG_DELETE,
            WriteType::Lock => FLAG_LOCK,
            WriteType::Rollback => FLAG_ROLLBACK,
        }
    }
}

#[derive(PartialEq, Clone)]
pub struct Write {
    pub write_type: WriteType,
    pub start_ts: TimeStamp,
    pub short_value: Option<Value>,

    /// The `commit_ts` of transactions can be non-globally-unique. But since we store Rollback
    /// records in the same CF where Commit records is, and Rollback records are saved with
    /// `user_key{start_ts}` as the internal key, the collision between Commit and Rollback
    /// records can't be avoided. In this case, we keep the Commit record, and set the
    /// `has_overlapped_rollback` flag to indicate that there's also a Rollback record.
    /// Also note that `has_overlapped_rollback` field is only necessary when the Rollback record
    /// should be protected.
    pub has_overlapped_rollback: bool,

    /// Records the next version after this version when overlapping rollback happens on an already
    /// existed commit record.
    ///
    /// When a rollback flag is written on an already-written commit record, it causes rewriting
    /// the commit record. It may cause problems with the GC compaction filter. Consider this case:
    ///
    /// ```text
    /// Key_100_put, Key_120_del
    /// ```
    ///
    /// and a rollback on `100` happens:
    ///
    /// ```text
    /// Key_100_put_R, Key_120_del
    /// ```
    ///
    /// Then GC with safepoint = 130 may happen. However a follower may not have finished applying
    /// the change. So on the follower, it's possible that:
    ///
    /// 1. `Key_100_put`, `Key_120_del` applied
    /// 2. GC with safepoint = 130 started and `Key_100_put`, `Key_120_del` are deleted
    /// 3. Finished applying `Key_100_put_R`, which means to rewrite `Key_100_put`
    /// 4. Read at `140` should get nothing (since it's MVCC-deleted at 120) but finds `Key_100_put`
    ///
    /// To solve the problem, when marking `has_overlapped_rollback` on an already-existed commit
    /// record, add a special field `gc_fence` on it. If there is a newer version after the record
    /// being rewritten, the next version's `commit_ts` will be recorded. When MVCC reading finds
    /// a commit record with a GC fence timestamp but the corresponding version that matches that ts
    /// doesn't exist, the current version will be believed to be already GC-ed and ignored.
    ///
    /// Therefore, for the example above, in the 3rd step it will record the version `120` to the
    /// `gc_fence` field:
    ///
    /// ```text
    /// Key_100_put_R_120, Key_120_del
    /// ```
    ///
    /// And when the reading in the 4th step finds the `PUT` record but the version at 120 doesn't
    /// exist, it will be regarded as already GC-ed and ignored.
    ///
    /// For CDC and TiFlash, when they receives a commit record with `gc_fence` field set, it can
    /// determine that it must be caused by an overlapped rollback instead of an actual commit.
    ///
    /// Note: GC fence will only be written on `PUT` and `DELETE` versions, and may only point to
    /// a `PUT` or `DELETE` version. If there are other `Lock` and `Rollback` records after the
    /// record that's being rewritten, they will be skipped. For example, in this case:
    ///
    /// ```text
    /// Key_100_put, Key_105_lock, Key_110_rollback, Key_120_del
    /// ```
    ///
    /// If overlapped rollback happens at 100, the `Key_100_put` will be rewritten as
    /// `Key_100_put_R_120`. It points to version 120 instead of the nearest 105.
    ///
    ///
    /// The meaning of the field:
    /// * `None`: A record that haven't been rewritten
    /// * `Some(0)`: A commit record that has been rewritten due to overlapping rollback, but it
    ///   doesn't have an newer version.
    /// * `Some(ts)`: A commit record that has been rewritten due to overlapping rollback,
    ///   and it's next version's `commit_ts` is `ts`
    pub gc_fence: Option<TimeStamp>,
}

impl std::fmt::Debug for Write {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Write")
            .field("write_type", &self.write_type)
            .field("start_ts", &self.start_ts)
            .field(
                "short_value",
                &self
                    .short_value
                    .as_ref()
                    .map(|x| &x[..])
                    .map(log_wrappers::Value::value)
                    .map(|x| format!("{:?}", x))
                    .unwrap_or_else(|| "None".to_owned()),
            )
            .field("has_overlapped_rollback", &self.has_overlapped_rollback)
            .field("gc_fence", &self.gc_fence)
            .finish()
    }
}

impl Write {
    /// Creates a new `Write` record.
    #[inline]
    pub fn new(write_type: WriteType, start_ts: TimeStamp, short_value: Option<Value>) -> Write {
        Write {
            write_type,
            start_ts,
            short_value,
            has_overlapped_rollback: false,
            gc_fence: None,
        }
    }

    #[inline]
    pub fn new_rollback(start_ts: TimeStamp, protected: bool) -> Write {
        let short_value = if protected {
            Some(PROTECTED_ROLLBACK_SHORT_VALUE.to_vec())
        } else {
            None
        };

        Write {
            write_type: WriteType::Rollback,
            start_ts,
            short_value,
            has_overlapped_rollback: false,
            gc_fence: None,
        }
    }

    #[inline]
    pub fn set_overlapped_rollback(
        mut self,
        has_overlapped_rollback: bool,
        gc_fence: Option<TimeStamp>,
    ) -> Self {
        self.has_overlapped_rollback = has_overlapped_rollback;
        self.gc_fence = gc_fence;
        self
    }

    #[inline]
    pub fn parse_type(mut b: &[u8]) -> Result<WriteType> {
        let write_type_bytes = b
            .read_u8()
            .map_err(|_| Error::from(ErrorInner::BadFormatWrite))?;
        WriteType::from_u8(write_type_bytes).ok_or_else(|| Error::from(ErrorInner::BadFormatWrite))
    }

    #[inline]
    pub fn as_ref(&self) -> WriteRef<'_> {
        WriteRef {
            write_type: self.write_type,
            start_ts: self.start_ts,
            short_value: self.short_value.as_deref(),
            has_overlapped_rollback: self.has_overlapped_rollback,
            gc_fence: self.gc_fence,
        }
    }

    pub fn may_have_old_value(&self) -> bool {
        matches!(self.write_type, WriteType::Put | WriteType::Delete)
    }
}

#[derive(PartialEq, Clone)]
pub struct WriteRef<'a> {
    pub write_type: WriteType,
    pub start_ts: TimeStamp,
    pub short_value: Option<&'a [u8]>,
    /// The `commit_ts` of transactions can be non-globally-unique. But since we store Rollback
    /// records in the same CF where Commit records is, and Rollback records are saved with
    /// `user_key{start_ts}` as the internal key, the collision between Commit and Rollback
    /// records can't be avoided. In this case, we keep the Commit record, and set the
    /// `has_overlapped_rollback` flag to indicate that there's also a Rollback record.
    /// Also note that `has_overlapped_rollback` field is only necessary when the Rollback record
    /// should be protected.
    pub has_overlapped_rollback: bool,

    /// Records the next version after this version when overlapping rollback happens on an already
    /// existed commit record.
    ///
    /// See [`Write::gc_fence`] for more detail.
    pub gc_fence: Option<TimeStamp>,
}

impl WriteRef<'_> {
    pub fn parse(mut b: &[u8]) -> Result<WriteRef<'_>> {
        let write_type_bytes = b
            .read_u8()
            .map_err(|_| Error::from(ErrorInner::BadFormatWrite))?;
        let write_type = WriteType::from_u8(write_type_bytes)
            .ok_or_else(|| Error::from(ErrorInner::BadFormatWrite))?;
        let start_ts = b
            .read_var_u64()
            .map_err(|_| Error::from(ErrorInner::BadFormatWrite))?
            .into();

        let mut short_value = None;
        let mut has_overlapped_rollback = false;
        let mut gc_fence = None;

        while !b.is_empty() {
            match b
                .read_u8()
                .map_err(|_| Error::from(ErrorInner::BadFormatWrite))?
            {
                SHORT_VALUE_PREFIX => {
                    let len = b
                        .read_u8()
                        .map_err(|_| Error::from(ErrorInner::BadFormatWrite))?;
                    if b.len() < len as usize {
                        panic!(
                            "content len [{}] shorter than short value len [{}]",
                            b.len(),
                            len,
                        );
                    }
                    short_value = Some(&b[..len as usize]);
                    b = &b[len as usize..];
                }
                FLAG_OVERLAPPED_ROLLBACK => {
                    has_overlapped_rollback = true;
                }
                GC_FENCE_PREFIX => gc_fence = Some(number::decode_u64(&mut b)?.into()),
                _ => {
                    // To support forward compatibility, all fields should be serialized in order
                    // and stop parsing if meets an unknown byte.
                    break;
                }
            }
        }

        Ok(WriteRef {
            write_type,
            start_ts,
            short_value,
            has_overlapped_rollback,
            gc_fence,
        })
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(self.pre_allocate_size());
        b.push(self.write_type.to_u8());
        b.encode_var_u64(self.start_ts.into_inner()).unwrap();
        if let Some(v) = self.short_value {
            b.push(SHORT_VALUE_PREFIX);
            b.push(v.len() as u8);
            b.extend_from_slice(v);
        }
        if self.has_overlapped_rollback {
            b.push(FLAG_OVERLAPPED_ROLLBACK);
        }
        if let Some(ts) = self.gc_fence {
            b.push(GC_FENCE_PREFIX);
            b.encode_u64(ts.into_inner()).unwrap();
        }
        b
    }

    fn pre_allocate_size(&self) -> usize {
        let mut size = 1 + MAX_VAR_U64_LEN + self.has_overlapped_rollback as usize;

        if let Some(v) = &self.short_value {
            size += 2 + v.len();
        }
        if self.gc_fence.is_some() {
            size += 1 + size_of::<u64>();
        }
        size
    }

    /// Prev Conditions:
    ///   * The `Write` record `self` is referring to is the latest version found by reading at `read_ts`
    ///   * The `read_ts` is safe, which means, it's not earlier than the current GC safepoint.
    /// Return:
    ///   Whether the `Write` record is valid, ie. there's no GC fence or GC fence doesn't points to any other
    ///   version.
    pub fn check_gc_fence_as_latest_version(&self, read_ts: TimeStamp) -> bool {
        // It's a valid write record if there's no GC fence or GC fence doesn't points to any other
        // version.
        // If there is a GC fence that's points to another version, there are two cases:
        // * If `gc_fence_ts > read_ts`, then since `read_ts` didn't expire the GC
        //   safepoint, so the current version must be a not-expired version or the latest version
        //   before safepoint, so it must be a valid version
        // * If `gc_fence_ts <= read_ts`, since the current version is the latest version found by
        //   reading at `read_ts`, the version at `gc_fence_ts` must be missing, so the current
        //   version must be invalid.
        if let Some(gc_fence_ts) = self.gc_fence {
            if !gc_fence_ts.is_zero() && gc_fence_ts <= read_ts {
                return false;
            }
        }

        true
    }

    #[inline]
    pub fn is_protected(&self) -> bool {
        self.write_type == WriteType::Rollback
            && self
                .short_value
                .as_ref()
                .map(|v| *v == PROTECTED_ROLLBACK_SHORT_VALUE)
                .unwrap_or_default()
    }

    #[inline]
    pub fn to_owned(&self) -> Write {
        Write::new(
            self.write_type,
            self.start_ts,
            self.short_value.map(|v| v.to_owned()),
        )
        .set_overlapped_rollback(self.has_overlapped_rollback, self.gc_fence)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_type() {
        let mut tests = vec![
            (Some(LockType::Put), WriteType::Put, FLAG_PUT),
            (Some(LockType::Delete), WriteType::Delete, FLAG_DELETE),
            (Some(LockType::Lock), WriteType::Lock, FLAG_LOCK),
            (None, WriteType::Rollback, FLAG_ROLLBACK),
        ];
        for (i, (lock_type, write_type, flag)) in tests.drain(..).enumerate() {
            if let Some(lock_type) = lock_type {
                let wt = WriteType::from_lock_type(lock_type).unwrap();
                assert_eq!(
                    wt, write_type,
                    "#{}, expect from_lock_type({:?}) returns {:?}, but got {:?}",
                    i, lock_type, write_type, wt
                );
            }
            let f = write_type.to_u8();
            assert_eq!(
                f, flag,
                "#{}, expect {:?}.to_u8() returns {:?}, but got {:?}",
                i, write_type, flag, f
            );
            let wt = WriteType::from_u8(flag).unwrap();
            assert_eq!(
                wt, write_type,
                "#{}, expect from_u8({:?}) returns {:?}, but got {:?}",
                i, flag, write_type, wt
            );
        }
    }

    #[test]
    fn test_write() {
        // Test `Write::to_bytes()` and `Write::parse()` works as a pair.
        let mut writes = vec![
            Write::new(WriteType::Put, 0.into(), Some(b"short_value".to_vec())),
            Write::new(WriteType::Delete, (1 << 20).into(), None),
            Write::new_rollback((1 << 40).into(), true),
            Write::new(WriteType::Rollback, (1 << 41).into(), None),
            Write::new(WriteType::Put, 123.into(), None).set_overlapped_rollback(true, None),
            Write::new(WriteType::Put, 123.into(), None)
                .set_overlapped_rollback(true, Some(1234567.into())),
            Write::new(WriteType::Put, 456.into(), Some(b"short_value".to_vec()))
                .set_overlapped_rollback(true, None),
            Write::new(WriteType::Put, 456.into(), Some(b"short_value".to_vec()))
                .set_overlapped_rollback(true, Some(0.into())),
            Write::new(WriteType::Put, 456.into(), Some(b"short_value".to_vec()))
                .set_overlapped_rollback(true, Some(2345678.into())),
            Write::new(WriteType::Put, 456.into(), Some(b"short_value".to_vec()))
                .set_overlapped_rollback(true, Some(421397468076048385.into())),
        ];
        for (i, write) in writes.drain(..).enumerate() {
            let v = write.as_ref().to_bytes();
            assert!(v.len() <= write.as_ref().pre_allocate_size());
            let w = WriteRef::parse(&v[..])
                .unwrap_or_else(|e| panic!("#{} parse() err: {:?}", i, e))
                .to_owned();
            assert_eq!(w, write, "#{} expect {:?}, but got {:?}", i, write, w);
            assert_eq!(Write::parse_type(&v).unwrap(), w.write_type);
        }

        // Test `Write::parse()` handles incorrect input.
        assert!(WriteRef::parse(b"").is_err());

        let lock = Write::new(WriteType::Lock, 1.into(), Some(b"short_value".to_vec()));
        let mut v = lock.as_ref().to_bytes();
        assert!(WriteRef::parse(&v[..1]).is_err());
        assert_eq!(Write::parse_type(&v).unwrap(), lock.write_type);
        // Test `Write::parse()` ignores unknown bytes.
        v.extend(b"unknown");
        let w = WriteRef::parse(&v).unwrap().to_owned();
        assert_eq!(w, lock);
    }

    #[test]
    fn test_is_protected() {
        assert!(Write::new_rollback(1.into(), true).as_ref().is_protected());
        assert!(!Write::new_rollback(2.into(), false).as_ref().is_protected());
        assert!(!Write::new(
            WriteType::Put,
            3.into(),
            Some(PROTECTED_ROLLBACK_SHORT_VALUE.to_vec()),
        )
        .as_ref()
        .is_protected());
    }

    #[test]
    fn test_check_gc_fence() {
        // (gc_fence, read_ts, expected_result).
        let cases: Vec<(Option<u64>, u64, bool)> = vec![
            (None, 10, true),
            (None, 100, true),
            (None, u64::max_value(), true),
            (Some(0), 100, true),
            (Some(0), u64::max_value(), true),
            (Some(100), 50, true),
            (Some(100), 100, false),
            (Some(100), 150, false),
            (Some(100), u64::max_value(), false),
        ];

        for case in cases {
            let write = Write::new(WriteType::Put, 5.into(), None)
                .set_overlapped_rollback(true, case.0.map(Into::into));

            assert_eq!(
                write
                    .as_ref()
                    .check_gc_fence_as_latest_version(case.1.into()),
                case.2
            );
        }
    }
}
