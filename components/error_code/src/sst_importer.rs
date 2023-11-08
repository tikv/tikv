// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

define_error_codes!(
    "KV:SstImporter:",

    IO => ("Io", "", ""),
    GRPC => ("Grpc", "", ""),
    UUID => ("Uuid", "", ""),
    FUTURE => ("Future", "", ""),
    ROCKSDB => ("RocksDb", "", ""),
    PARSE_INT_ERROR => ("ParseIntError", "", ""),
    FILE_EXISTS => ("FileExists", "", ""),
    FILE_CORRUPTED => ("FileCorrupted", "", ""),
    INVALID_SST_PATH => ("InvalidSstPath", "",""),
    INVALID_CHUNK => ("InvalidChunk", "", ""),
    ENGINE => ("Engine", "", ""),
    CANNOT_READ_EXTERNAL_STORAGE => ("CannotReadExternalStorage", "", ""),
    WRONG_KEY_PREFIX => ("WrongKeyPrefix", "", ""),
    BAD_FORMAT => ("BadFormat", "", ""),
    FILE_CONFLICT => ("FileConflict", "", ""),
    TTL_NOT_ENABLED => ("TtlNotEnabled", "", ""),
    TTL_LEN_NOT_EQUALS_TO_PAIRS => ("TtlLenNotEqualsToPairs", "", ""),
    INCOMPATIBLE_API_VERSION => ("IncompatibleApiVersion", "", ""),
    INVALID_KEY_MODE => ("InvalidKeyMode", "", ""),
    RESOURCE_NOT_ENOUTH => ("ResourceNotEnough", "", ""),
    SUSPENDED => ("Suspended",
        "this request has been suspended.",
        "Probably there are some export tools don't support exporting data inserted by `ingest`(say, snapshot backup). Check the user manual and stop them."),
    REQUEST_TOO_NEW => ("RequestTooNew", "", ""),
    REQUEST_TOO_OLD => ("RequestTooOld", "", "")
);
