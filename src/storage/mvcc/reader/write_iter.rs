// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use storage::mvcc::Result;
use storage::mvcc::Write;
use storage::{Cursor, Iterator, Key, Statistics};

/// An iterator to iterate all `Write`s for the given key whose commit_ts <= `max_ts`.
///
/// The return element will be `Some(Err(..))` if there are errors and there will
/// be no more future items.
///
/// The return element will be `Some(Ok((commit_ts, write)` if a `Write` is got.
///
/// Iterating will be started faster if the write cursor is near. Internally, there
/// will be `near_seek` for the first iterate and only `next` for later iterates.
pub struct ForwardWriteIter<'a, 'b, I: Iterator + 'a> {
    write_cursor: &'a mut Cursor<I>,
    key: &'b Key,
    statistics: Statistics,

    /// When we meet errors or there is no more results, future iterations should
    /// not yield results any more.
    is_ended: bool,
}

impl<'a, 'b, I: Iterator + 'a> ForwardWriteIter<'a, 'b, I> {
    /// Create a new `ForwardWriteIter`.
    pub fn new(
        write_cursor: &'a mut Cursor<I>, // TODO: make it `ForwardCursor`.
        key: &'b Key,
        max_ts: u64,
    ) -> Result<Self> {
        // seek to `${key}_${max_ts}` first.
        let mut statistics = Statistics::default();
        let is_ended = !write_cursor.near_seek(&key.append_ts(max_ts), &mut statistics.write)?;
        Ok(ForwardWriteIter {
            write_cursor,
            key,
            statistics,
            is_ended,
        })
    }

    /// Take out and reset the statistics collected so far.
    #[inline]
    pub fn take_statistics(&mut self) -> Statistics {
        ::std::mem::replace(&mut self.statistics, Statistics::default())
    }

    /// Iterate next `Write`.
    ///
    /// To be convenient to use `?` operator, the signature is `Result<Option<...>>` instead
    /// of what `::std::iter::Iterator::next` accepts, i.e. `Option<Result<...>>`.
    ///
    /// The meanings of the result value are:
    ///
    /// - `Result::Err(..)`: There is an error during this iterate. The whole iteration
    ///   should stop.
    /// - `Result::Ok(Some(..))`: We got a `Write` for this iterate. There may be more
    ///   values.
    /// - `Result::Ok(None)`: We got nothing for this iterate. The whole iteration should
    ///   stop.
    #[inline]
    fn internal_next(&mut self) -> Result<Option<(u64, Write)>> {
        if !self.write_cursor.valid() {
            // Key space ended.
            return Ok(None);
        }
        // We may move forward to another key. In this case, the scan ends.
        let write_key =
            Key::from_encoded(self.write_cursor.key(&mut self.statistics.write).to_vec());
        let commit_ts = write_key.decode_ts()?;
        let user_key = write_key.truncate_ts()?;
        if &user_key != self.key {
            // Moved to another key.
            return Ok(None);
        }
        let write = Write::parse(self.write_cursor.value(&mut self.statistics.write))?;
        self.statistics.write.processed += 1;
        self.write_cursor.next(&mut self.statistics.write);
        Ok(Some((commit_ts, write)))
    }
}

impl<'a, 'b, I: Iterator + 'a> ::std::iter::Iterator for ForwardWriteIter<'a, 'b, I> {
    type Item = Result<(u64, Write)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.is_ended {
            return None;
        }
        match self.internal_next() {
            Err(e) => {
                self.is_ended = true;
                Some(Err(e))
            }
            Ok(Some(v)) => Some(Ok(v)),
            Ok(None) => {
                self.is_ended = true;
                None
            }
        }
    }
}
