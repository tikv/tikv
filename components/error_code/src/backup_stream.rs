// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

define_error_codes! {
    "KV:LogBackup:",

    ETCD => ("ETCD",
        "Error during requesting the meta store(etcd)",
        "Please check the connectivity between TiKV and PD."),
    PROTO => ("Proto",
        "Error during decode / encoding protocol buffer messages",
        "Please check the version of TiKV / BR are compatible, or whether data is corrupted."
    ),
    NO_SUCH_TASK => ("NoSuchTask",
        "A task not found.",
        "Please check the spell of your task name."
    ),
    OBSERVE_CANCELED => (
        "ObserveCancel",
        "When doing initial scanning, the observe of that region has been canceled",
        "No need to handle this, this is retryable."
    ),
    MALFORMED_META => ("MalformedMetadata",
        "Malformed metadata found.",
        "The metadata format is unexpected, please check the compatibility between TiKV / BR."
    ),
    IO => ("IO",
        "Error during doing Input / Output operations.",
        "This is a generic error, please check the error message for further information."
    ),
    TXN => ("Txn",
        "Error during reading transaction data.",
        "This is an internal error, please ask the community for help."
    ),
    SCHED => ("Scheduler",
        "Error during scheduling internal task.",
        "This is an internal error, and may happen if there are too many changes to observe, please ask the community for help."
    ),
    PD => ("PD",
        "Error during requesting the Placement Driver.",
        "Please check the connectivity between TiKV and PD."
    ),
    RAFTREQ => ("RaftReq",
        "Error happened when sending raft command.",
        "This is an internal error, please ask the community for help."
    ),
    RAFTSTORE => ("RaftStore",
        "Error happened reported from raft store.",
        "This is an internal error, please ask the community for help."
    ),
    OTHER => ("Unknown",
        "Some random error happens.",
        "This is an generic error, please check the error message for further information."
    )
}
