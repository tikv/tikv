// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

define_error_codes!(
    "KV:Cloud:",

    IO => ("IO", "", ""),
    SSL => ("SSL", "", ""),
    PROTO => ("Proto", "", ""),
    UNKNOWN => ("Unknown", "", ""),
    TIMEOUT => ("Timeout", "", ""),
    INVALID_INPUT => ("Invalid input", "", ""),
    API_INTERNAL => ("API internal", "", ""),
    WRONG_MASTER_KEY => ("WrongMasterKey", "", ""),
    BOTH_MASTER_KEY_FAIL => ("BothMasterKeyFail", "", "")
);
