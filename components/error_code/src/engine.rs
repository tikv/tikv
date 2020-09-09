// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

define_error_codes!(
    "KV-Engine-",

    ENGINE => ("Engine", "", ""),
    NOT_IN_RANGE => ("NotInRange", "", ""),
    PROTOBUF => ("Protobuf", "", ""),
    IO => ("IO", "", ""),
    CF_NAME => ("CFName", "", ""),
    CODEC => ("Codec", "", ""),
<<<<<<< HEAD

    UNKNOWN => ("Unknown", "", "")
=======
    DATALOSS => ("DataLoss", "", ""),
    DATACOMPACTED => ("DataCompacted", "", "")
>>>>>>> 3f94eb8... *: output error code to error logs (#8595)
);
