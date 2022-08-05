// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

define_error_codes!(
    "KV:CausalTs:",

    PD => ("PdClient", "", ""),
    TSO => ("Tso", "", ""),
    TSO_BATCH_USED_UP => ("TsoBatchUsedUp", "", ""),
    BATCH_RENEW => ("BatchRenew", "", ""),

    UNKNOWN => ("Unknown", "", "")
);
