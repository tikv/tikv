// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use lazy_static::*;
use prometheus::*;
use prometheus_static_metric::*;

lazy_static! {
    pub static ref TS_PROVIDER_TSO_BATCH_SIZE: IntGauge = register_int_gauge!(
        "tikv_causal_ts_provider_tso_batch_size",
        "TSO batch size of causal timestamp provider"
    )
    .unwrap();
    pub static ref TS_PROVIDER_GET_TS_DURATION: HistogramVec = register_histogram_vec!(
        "tikv_causal_ts_provider_get_ts_duration_seconds",
        "Histogram of the duration of get_ts",
        &["result"],
        exponential_buckets(1e-7, 2.0, 20).unwrap() // 100ns ~ 100ms
    )
    .unwrap();
    pub static ref TS_PROVIDER_TSO_BATCH_RENEW_DURATION: HistogramVec = register_histogram_vec!(
        "tikv_causal_ts_provider_tso_batch_renew_duration_seconds",
        "Histogram of the duration of TSO batch renew",
        &["result", "reason"],
        exponential_buckets(1e-4, 2.0, 20).unwrap() // 0.1ms ~ 104s
    )
    .unwrap();
    pub static ref TS_PROVIDER_TSO_BATCH_LIST_COUNTING: HistogramVec = register_histogram_vec!(
        "tikv_causal_ts_provider_tso_batch_list_counting",
        "Histogram of TSO batch list counting",
        &["type"],
        exponential_buckets(10.0, 2.0, 20).unwrap() // 10 ~ 10,000,000
    )
    .unwrap();
}

make_auto_flush_static_metric! {
    pub label_enum TsoBatchRenewReason {
        init,
        background,
        used_up,
        flush,
    }

    pub label_enum TsoBatchCountingKind {
        tso_usage,
        tso_remain,
        new_batch_size,
    }

    pub label_enum ResultKind {
        ok,
        err,
    }

    pub struct TsProviderGetTsDurationVec: LocalHistogram {
        "result" => ResultKind,
    }

    pub struct TsoBatchRenewDurationVec: LocalHistogram {
        "result" => ResultKind,
        "reason" => TsoBatchRenewReason,
    }

    pub struct TsoBatchListCountingVec: LocalHistogram {
        "type" => TsoBatchCountingKind,
    }
}

impl<T, E> From<&std::result::Result<T, E>> for ResultKind {
    #[inline]
    fn from(res: &std::result::Result<T, E>) -> Self {
        if res.is_ok() { Self::ok } else { Self::err }
    }
}

lazy_static! {
    pub static ref TS_PROVIDER_GET_TS_DURATION_STATIC: TsProviderGetTsDurationVec =
        auto_flush_from!(TS_PROVIDER_GET_TS_DURATION, TsProviderGetTsDurationVec);
    pub static ref TS_PROVIDER_TSO_BATCH_RENEW_DURATION_STATIC: TsoBatchRenewDurationVec = auto_flush_from!(
        TS_PROVIDER_TSO_BATCH_RENEW_DURATION,
        TsoBatchRenewDurationVec
    );
    pub static ref TS_PROVIDER_TSO_BATCH_LIST_COUNTING_STATIC: TsoBatchListCountingVec =
        auto_flush_from!(TS_PROVIDER_TSO_BATCH_LIST_COUNTING, TsoBatchListCountingVec);
}
