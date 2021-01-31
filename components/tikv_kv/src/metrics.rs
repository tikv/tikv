// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use prometheus_static_metric::*;

make_auto_flush_static_metric! {
    pub label_enum GcKeysCF {
        default,
        lock,
        write,
    }

    pub label_enum GcKeysDetail {
        processed_keys,
        get,
        next,
        prev,
        seek,
        seek_for_prev,
        over_seek_bound,
        next_tombstone,
        prev_tombstone,
        seek_tombstone,
        seek_for_prev_tombstone,
    }

    pub struct GcKeysCounterVec: LocalIntCounter {
        "cf" => GcKeysCF,
        "tag" => GcKeysDetail,
    }
}
