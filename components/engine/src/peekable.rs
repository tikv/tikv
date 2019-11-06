// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

pub use crate::rocks::DBVector;
use crate::Result;
use keys::{PhysicalKey, ToPhysicalKeySlice};

// TODO: refactor this trait into rocksdb trait.
pub trait Peekable {
    type Key: PhysicalKey;

    fn get_value(
        &self,
        key: impl ToPhysicalKeySlice<<Self::Key as PhysicalKey>::Slice>,
    ) -> Result<Option<DBVector>>;

    fn get_value_cf(
        &self,
        cf: &str,
        key: impl ToPhysicalKeySlice<<Self::Key as PhysicalKey>::Slice>,
    ) -> Result<Option<DBVector>>;

    fn get_msg<M: protobuf::Message + Default, K>(&self, key: K) -> Result<Option<M>>
    where
        K: ToPhysicalKeySlice<<Self::Key as PhysicalKey>::Slice>,
    {
        let value = self.get_value(key)?;

        if value.is_none() {
            return Ok(None);
        }

        let mut m = M::default();
        m.merge_from_bytes(&value.unwrap())?;
        Ok(Some(m))
    }

    fn get_msg_cf<M: protobuf::Message + Default, K>(&self, cf: &str, key: K) -> Result<Option<M>>
    where
        K: ToPhysicalKeySlice<<Self::Key as PhysicalKey>::Slice>,
    {
        let value = self.get_value_cf(cf, key)?;

        if value.is_none() {
            return Ok(None);
        }

        let mut m = M::default();
        m.merge_from_bytes(&value.unwrap())?;
        Ok(Some(m))
    }
}
