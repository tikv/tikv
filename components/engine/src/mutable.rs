// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::rocks::{CFHandle, Writable, WriteBatchBase};
use crate::Result;

pub trait Immutable: Writable {
    fn put_msg<M: protobuf::Message>(&self, key: &[u8], m: &M) -> Result<()> {
        let value = m.write_to_bytes()?;
        self.put(key, &value)?;
        Ok(())
    }

    // TODO: change CFHandle to str.
    fn put_msg_cf<M: protobuf::Message>(&self, cf: &CFHandle, key: &[u8], m: &M) -> Result<()> {
        let value = m.write_to_bytes()?;
        self.put_cf(cf, key, &value)?;
        Ok(())
    }

    fn del(&self, key: &[u8]) -> Result<()> {
        self.delete(key)?;
        Ok(())
    }
}

pub trait Mutable: WriteBatchBase {
    fn put_msg<M: protobuf::Message>(&mut self, key: &[u8], m: &M) -> Result<()> {
        let value = m.write_to_bytes()?;
        self.put(key, &value)?;
        Ok(())
    }

    // TODO: change CFHandle to str.
    fn put_msg_cf<M: protobuf::Message>(&mut self, cf: &CFHandle, key: &[u8], m: &M) -> Result<()> {
        let value = m.write_to_bytes()?;
        self.put_cf(cf, key, &value)?;
        Ok(())
    }

    fn del(&mut self, key: &[u8]) -> Result<()> {
        self.delete(key)?;
        Ok(())
    }
}
