// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

define_error_codes!(
    "KV:CausalTs:",

    PD => ("PdClient", "", ""),
    TSO => ("TSO", "", ""),
    TSO_BATCH_USED_UP => ("TSO batch used up", "", ""),
    BATCH_RENEW => ("Batch renew", "", ""),

    UNKNOWN => ("Unknown", "", "")
);
