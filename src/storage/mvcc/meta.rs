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
use kvproto::mvccpb::{Meta as PbMeta, MetaItem, MetaLock};
use super::Result;

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

    pub fn get_lock(&self) -> Option<&MetaLock> {
        if self.pb.has_lock() {
            Some(self.pb.get_lock())
        } else {
            None
        }
    }

    pub fn set_lock(&mut self, lock: MetaLock) {
        self.pb.set_lock(lock);
    }

    pub fn clear_lock(&mut self) {
        self.pb.clear_lock();
    }

    pub fn iter_items(&self) -> Iter<MetaItem> {
        self.pb.get_items().iter()
    }

    pub fn get_item_by_start_ts(&self, ts: u64) -> Option<&MetaItem> {
        self.iter_items().take_while(|x| x.get_start_ts() >= ts).find(|x| x.get_start_ts() == ts)
    }

    pub fn push_item(&mut self, item: MetaItem) {
        self.pb.mut_items().insert(0, item);
    }
}

#[cfg(test)]
mod tests {
    use super::Meta;
    use kvproto::mvccpb::{MetaLock, MetaLockType, MetaItem};

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

        let item = meta2.get_item_by_start_ts(3).unwrap();
        assert_eq!(item.get_start_ts(), 3);
        assert!(meta2.get_item_by_start_ts(4).is_none());
        assert!(meta2.get_item_by_start_ts(0).is_none());
    }

    #[test]
    fn test_meta_lock() {
        let mut meta = Meta::new();
        assert!(meta.get_lock().is_none());
        let mut lock = MetaLock::new();
        lock.set_field_type(MetaLockType::ReadWrite);
        lock.set_start_ts(1);
        lock.set_primary_key(b"pk".to_vec());
        meta.set_lock(lock);

        {
            let lock = meta.get_lock().unwrap();
            assert_eq!(lock.get_start_ts(), 1);
            assert_eq!(lock.get_primary_key(), b"pk");
            assert_eq!(lock.get_field_type(), MetaLockType::ReadWrite);
        }

        meta.clear_lock();
        assert!(meta.get_lock().is_none());
    }
}
