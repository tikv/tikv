// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::KvEngine;

mod compact;

pub enum Task {
    Compact(CompactTask),
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Task::Compact(ref t) => t.fmt(f),
        }
    }
}

pub struct Runner<E: KvEngine> {
    compact: CompactRunner<E>,
}

impl Runner {}
