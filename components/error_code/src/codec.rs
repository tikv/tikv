// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

define_error_codes!(
    "KV:Codec:",

    IO => ("Io", "", ""),
    BAD_PADDING => ("BadPadding", "", ""),
    KEY_LENGTH => ("KeyLength", "", ""),
    KEY_NOT_FOUND => ("KeyNotFound", "", "")
);
