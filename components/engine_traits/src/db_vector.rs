// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::Debug;
use std::ops::Deref;

/// A type that holds buffers queried from the database.
///
/// The database may optimize this type to be a view into
/// its own cache.
pub trait DBVector: Debug + Deref<Target = [u8]> + for<'a> PartialEq<&'a [u8]> {}
