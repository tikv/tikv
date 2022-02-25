// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

define_error_codes!(
    "KV:Cloud:",

    IO => ("IO", "", ""),
    SSL => ("SSL", "", ""),
    PROTO => ("Proto", "", ""),
    UNKNOWN => ("Unknown", "", ""),
    TIMEOUT => ("Timeout", "", ""),
    INVALID_INPUT => ("InvalidInput", "", ""),
    API_INTERNAL => ("ApiInternal", "", ""),
    API_NOT_FOUND => ("ApiNotFound", "", ""),
    API_AUTHENTICATION => ("ApiAuthentication", "", ""),
    WRONG_MASTER_KEY => ("WrongMasterKey", "", ""),
    BOTH_MASTER_KEY_FAIL => ("BothMasterKeyFail", "", "")
);
