// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

define_error_codes!(
    "KV:Encryption:",

    ROCKS => ("Rocks", "", ""),
    IO => ("IO", "", ""),
    CRYPTER => ("Crypter", "", ""),
    PROTO => ("Proto", "", ""),
    UNKNOWN_ENCRYPTION => ("UnknownEncryption", "", ""),
    WRONG_MASTER_KEY => ("WrongMasterKey", "", ""),
    BOTH_MASTER_KEY_FAIL => ("BothMasterKeyFail", "", ""),
    PARSE_INCOMPLETE => ("TailRecordParseIncomplete", "", "")
);
