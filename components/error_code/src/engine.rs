// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

define_error_codes!(
    "KV:Engine:",

    ENGINE => ("Engine", "", ""),
    NOT_IN_RANGE => ("NotInRange", "", ""),
    PROTOBUF => ("Protobuf", "", ""),
    IO => ("IO", "", ""),
    CF_NAME => ("CFName", "", ""),
    CODEC => ("Codec", "", ""),
    DATALOSS => ("DataLoss", "", ""),
    DATACOMPACTED => ("DataCompacted", "", "")
);
