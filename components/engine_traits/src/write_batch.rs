// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::errors::Result;
use crate::mutable::Mutable;

pub trait WriteBatch: Mutable + Send {
    fn data_size(&self) -> usize;
    fn count(&self) -> usize;
    fn is_empty(&self) -> bool;
    fn clear(&self);

    fn set_save_point(&mut self);
    fn pop_save_point(&mut self) -> Result<()>;
    fn rollback_to_save_point(&mut self) -> Result<()>;
}
