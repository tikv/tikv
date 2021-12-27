// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Sync + Send>>;
