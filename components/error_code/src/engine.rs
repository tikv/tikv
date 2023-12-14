// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

define_error_codes!(
    "KV:Engine:",

    ENGINE => ("Engine", "", ""),
    NOT_IN_RANGE => ("NotInRange", "", ""),
    PROTOBUF => ("Protobuf", "", ""),
    IO => ("Io", "", ""),
    CF_NAME => ("CfName", "", ""),
    CODEC => ("Codec", "", ""),
    DATALOSS => ("DataLoss", "", ""),
    DATACOMPACTED => ("DataCompacted", "", ""),
    BOUNDARY_NOT_SET => ("BoundaryNotSet", "", "")
);
