// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::*;

pub trait SyncMutable {
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()>;

    fn put_cf(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<()>;

    fn delete(&self, key: &[u8]) -> Result<()>;

    fn delete_cf(&self, cf: &str, key: &[u8]) -> Result<()>;

    fn delete_range_cf(&self, cf: &str, begin_key: &[u8], end_key: &[u8]) -> Result<()>;

    fn put_msg<M: protobuf::Message>(&self, key: &[u8], m: &M) -> Result<()> {
        self.put(key, &m.write_to_bytes()?)
    }

    fn put_msg_cf<M: protobuf::Message>(&self, cf: &str, key: &[u8], m: &M) -> Result<()> {
        self.put_cf(cf, key, &m.write_to_bytes()?)
    }
}

pub trait Mutable {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()>;

    fn put_cf(&mut self, cf: &str, key: &[u8], value: &[u8]) -> Result<()>;

    fn delete(&mut self, key: &[u8]) -> Result<()>;

    fn delete_cf(&mut self, cf: &str, key: &[u8]) -> Result<()>;

    fn delete_range_cf(&mut self, cf: &str, begin_key: &[u8], end_key: &[u8]) -> Result<()>;

    fn put_msg<M: protobuf::Message>(&mut self, key: &[u8], m: &M) -> Result<()> {
        self.put(key, &m.write_to_bytes()?)
    }

    fn put_msg_cf<M: protobuf::Message>(&mut self, cf: &str, key: &[u8], m: &M) -> Result<()> {
        self.put_cf(cf, key, &m.write_to_bytes()?)
    }
}
