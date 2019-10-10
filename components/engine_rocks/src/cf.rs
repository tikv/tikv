// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::CFHandle;

pub struct RocksCFHandle;

impl CFHandle for RocksCFHandle { }
