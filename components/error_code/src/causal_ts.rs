// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

define_error_codes!(
    "KV:CausalTs:",

    PD => ("PdClient", "", ""),
    TSO => ("TSO", "", ""),
    HLC => ("HLC", "", ""),

    UNKNOWN => ("Unknown", "", "")
);
