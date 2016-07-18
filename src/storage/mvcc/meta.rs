// Copyright 2016 PingCAP, Inc.
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

use std::slice::Iter;
use protobuf::core::Message;
use protobuf::RepeatedField;
use kvproto::mvccpb::{Meta as PbMeta, MetaItem};
use super::Result;

pub const META_SPLIT_SIZE: usize = 5;
pub const META_RESERVE_SIZE: usize = 2;
pub const FIRST_META_INDEX: u64 = 0;

#[derive(Debug)]
pub struct Meta {
    pb: PbMeta,
}

impl Meta {
    pub fn new() -> Meta {
        Meta { pb: PbMeta::new() }
    }

    pub fn parse(data: &[u8]) -> Result<Meta> {
        let mut pb = PbMeta::new();
        try!(pb.merge_from_bytes(data));
        Ok(Meta { pb: pb })
    }

    pub fn write_to(&self, os: &mut Vec<u8>) {
        self.pb.write_to_vec(os).unwrap();
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut os = vec![];
        self.write_to(&mut os);
        os
    }

    pub fn iter_items(&self) -> Iter<MetaItem> {
        self.pb.get_items().iter()
    }

    pub fn push_item(&mut self, item: MetaItem) {
        self.pb.mut_items().insert(0, item);
    }

    pub fn next_index(&self) -> Option<u64> {
        match self.pb.get_next() {
            0 => None, // 0 means no more Meta.
            x => Some(x),
        }
    }

    pub fn split(&mut self) -> Option<(Meta, u64)> {
        if self.pb.get_items().len() < META_SPLIT_SIZE {
            return None;
        }
        let mut items = self.pb.take_items().into_vec();
        let new_items = items.split_off(META_RESERVE_SIZE);
        let index = self.pb.get_next();
        let next_index = index + 1;

        self.pb.set_items(RepeatedField::from_vec(items));
        self.pb.set_next(next_index);

        let mut new_meta = Meta::new();
        new_meta.pb.set_items(RepeatedField::from_vec(new_items));
        new_meta.pb.set_next(index);

        Some((new_meta, next_index))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ops::RangeFrom;
    use kvproto::mvccpb::MetaItem;
    use storage::mvcc::TEST_TS_BASE;

    #[test]
    fn test_meta() {
        let mut meta = Meta::new();
        meta.push_item({
            let mut item = MetaItem::new();
            item.set_start_ts(1);
            item.set_commit_ts(2);
            item
        });
        meta.push_item({
            let mut item = MetaItem::new();
            item.set_start_ts(3);
            item.set_commit_ts(4);
            item
        });

        let mut data = vec![];
        meta.write_to(&mut data);

        let meta2 = Meta::parse(&data).unwrap();
        let items: Vec<&_> = meta2.iter_items().collect();
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].get_start_ts(), 3);
        assert_eq!(items[0].get_commit_ts(), 4);
        assert_eq!(items[1].get_start_ts(), 1);
        assert_eq!(items[1].get_commit_ts(), 2);
    }

    #[test]
    fn test_meta_split() {
        let mut meta = Meta::new();
        let mut ts = TEST_TS_BASE..;

        push_item_n(&mut meta, &mut ts, META_SPLIT_SIZE - 1);
        assert!(meta.split().is_none());
        assert_eq!(meta.next_index(), None);

        push_item_n(&mut meta, &mut ts, 1);
        let (meta1, index) = meta.split().unwrap();
        assert_eq!(index, 1);
        assert_eq!(meta.next_index(), Some(1));
        assert_eq!(meta1.next_index(), None);

        push_item_n(&mut meta, &mut ts, META_SPLIT_SIZE);
        let (meta2, index) = meta.split().unwrap();
        assert_eq!(index, 2);
        assert_eq!(meta.next_index(), Some(2));
        assert_eq!(meta2.next_index(), Some(1));
    }

    fn push_item_n(meta: &mut Meta, ts: &mut RangeFrom<u64>, n: usize) {
        for _ in 0..n {
            let mut item = MetaItem::new();
            item.set_start_ts(ts.next().unwrap());
            item.set_commit_ts(ts.next().unwrap());
            meta.push_item(item);
        }
    }
}
