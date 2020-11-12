// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

pub trait PerfContextExt {
    type PerfContext: PerfContext;

    fn get() -> Option<Self::PerfContext>;
}

pub trait PerfContext {
}
