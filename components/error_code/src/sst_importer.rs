// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

define_error_codes!(
    "KV:SSTImporter:",

    IO => ("Io", "", ""),
    GRPC => ("gRPC", "", ""),
    UUID => ("Uuid", "", ""),
    FUTURE => ("Future", "", ""),
    ROCKSDB => ("RocksDB", "", ""),
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
    INVALID_KEY_MODE => ("InvalidKeyMode", "", "")
);
