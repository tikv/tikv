// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

define_error_codes!(
    "KV:PD:",

    IO => ("IO", "", ""),
    CLUSTER_BOOTSTRAPPED => ("ClusterBootstraped", "", ""),
    CLUSTER_NOT_BOOTSTRAPPED => ("ClusterNotBootstraped", "", ""),
    INCOMPATIBLE => ("Imcompatible", "", ""),
    GRPC => ("gRPC", "", ""),
    REGION_NOT_FOUND => ("RegionNotFound", "", ""),
    STORE_TOMBSTONE => ("StoreTombstone", "", ""),
    GLOBAL_CONFIG_NOT_FOUND => ("GlobalConfigNotFound","",""),
    UNKNOWN => ("Unknown", "", "")
);
