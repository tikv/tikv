// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

define_error_codes!(
    "KV:Coprocessor:",

    LOCKED => ("Locked", "", ""),
    DEADLINE_EXCEEDED => ("DeadlineExceeded", "", ""),
    MAX_PENDING_TASKS_EXCEEDED => ("MaxPendingTasksExceeded", "", ""),

    INVALID_DATA_TYPE => ("InvalidDataType", "", ""),
    ENCODING => ("Encoding", "", ""),
    COLUMN_OFFSET => ("ColumnOffset", "", ""),
    UNKNOWN_SIGNATURE => ("UnknownSignature", "", ""),
    EVAL => ("Eval", "", ""),
    CORRUPTED_DATA => ("CorruptedData", "", ""),

    STORAGE_ERROR => ("StorageError", "", ""),
    INVALID_CHARACTER_STRING => ("InvalidCharacterString", "", "")
);
