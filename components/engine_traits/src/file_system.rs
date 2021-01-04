// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

pub trait FileSystemInspector: Sync + Send {
    fn read(&self, len: usize) -> Result<usize, String>;
    fn write(&self, len: usize) -> Result<usize, String>;
}
