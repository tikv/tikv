// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

define_error_codes!(
    "KV:Storage:",

    TIMEOUT => ("Timeout", "", ""),
    EMPTY_REQUEST => ("EmptyRequest", "", ""),
    CLOSED => ("Closed", "", ""),
    IO => ("Io", "", ""),
    SCHED_TOO_BUSY => ("SchedTooBusy", "", ""),
    GC_WORKER_TOO_BUSY => ("GcWorkerTooBusy", "", ""),
    KEY_TOO_LARGE => ("KeyTooLarge", "", ""),
    INVALID_CF => ("InvalidCF", "", ""),
    PROTOBUF => ("Protobuf", "", ""),
    INVALID_TXN_TSO => ("INVALIDTXNTSO", "", ""),
    INVALID_REQ_RANGE => ("InvalidReqRange", "", ""),
    BAD_FORMAT_LOCK => ("BadFormatLock", "", ""),
    BAD_FORMAT_WRITE => ("BadFormatWrite", "",""),
    KEY_IS_LOCKED=> ("KeyIsLocked", "", ""),

    COMMITTED => ("Committed", "", ""),
    PESSIMISTIC_LOCK_ROLLED_BACK => ("PessimisticLockRolledBack", "", ""),
    TXN_LOCK_NOT_FOUND => ("TxnLockNotFound", "", ""),
    TXN_NOT_FOUND => ("TxnNotFound", "", ""),
    LOCK_TYPE_NOT_MATCH => ("LockTypeNotMatch", "", ""),
    WRITE_CONFLICT => ("WriteConflict", "", ""),
    DEADLOCK => ("Deadlock", "", ""),
    ALREADY_EXIST => ("AlreadyExist", "",""),
    DEFAULT_NOT_FOUND => ("DefaultNotFound", "", ""),
    COMMIT_TS_EXPIRED => ("CommitTsExpired", "", ""),
    KEY_VERSION => ("KeyVersion", "",""),
    PESSIMISTIC_LOCK_NOT_FOUND => ("PessimisticLockNotFound", "", ""),
    PESSIMISTIC_TXN_NOT_ENABLE => ("PessimisticTxnNotEnable", "", ""),

    UNKNOWN => ("Unknown", "", "")
);
