// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::*;

pub trait Mutable {
    fn put_opt(&mut self, opts: &WriteOptions, key: &[u8], value: &[u8]) -> Result<()>;
    fn put_cf_opt(&mut self, opts: &WriteOptions, cf: &str, key: &[u8], value: &[u8])
        -> Result<()>;

    fn delete_opt(&mut self, opts: &WriteOptions, key: &[u8]) -> Result<()>;
    fn delete_cf_opt(&mut self, opts: &WriteOptions, cf: &str, key: &[u8]) -> Result<()>;

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_opt(&WriteOptions::default(), key, value)
    }

    fn put_cf(&mut self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_cf_opt(&WriteOptions::default(), cf, key, value)
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.delete_opt(&WriteOptions::default(), key)
    }

    fn delete_cf(&mut self, cf: &str, key: &[u8]) -> Result<()> {
        self.delete_cf_opt(&WriteOptions::default(), cf, key)
    }

    fn put_msg<M: protobuf::Message>(&mut self, key: &[u8], m: &M) -> Result<()> {
        self.put(key, &m.write_to_bytes()?)
    }

    fn put_msg_cf<M: protobuf::Message>(&mut self, cf: &str, key: &[u8], m: &M) -> Result<()> {
        self.put_cf(cf, key, &m.write_to_bytes()?)
    }
}
