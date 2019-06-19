---
title: PD Recover User Guide
summary: Use PD Recover to recover a PD cluster which cannot start or provide services normally.
category: reference
---

# PD Recover User Guide

PD Recover is a disaster recovery tool of PD, used to recover the PD cluster which cannot start or provide services normally.

## Source code compiling

1. [Go](https://golang.org/) Version 1.11 or later
2. In the root directory of the [PD project](https://github.com/pingcap/pd), use the `make` command to compile and generate `bin/pd-recover`

## Usage

This section describes how to recover a PD cluster which cannot start or provide services normally.

### Flags description

```
-alloc-id uint
      Specify a number larger than the allocated ID of the original cluster
-cacert string
      Specify the path to the trusted CA certificate file in PEM format
-cert string
      Specify the path to the SSL certificate file in PEM format
-key string
      Specify the path to the SSL certificate key file in PEM format, which is the private key of the certificate specified by `--cert`
-cluster-id uint
      Specify the Cluster ID of the original cluster
-endpoints string
      Specify the PD address (default: "http://127.0.0.1:2379")
```

### Recovery flow

1. Obtain the Cluster ID and the Alloc ID from the current cluster.

    - Obtain the Cluster ID from the PD and TiKV logs.
    - Obtain the allocated Alloc ID from either the PD log or the `Metadata Information` in the PD monitoring panel.

    Specifying `alloc-id` requires a number larger than the current largest Alloc ID. If you fail to obtain the Alloc ID, you can make an estimate of a larger number according to the number of Regions and stores in the cluster. Generally, you can specify a number that is several orders of magnitude larger.

2. Stop the whole cluster, clear the PD data directory, and restart the PD cluster.
3. Use PD Recover to recover and make sure that you use the correct `cluster-id` and appropriate `alloc-id`.
4. When the recovery success information is prompted, restart the whole cluster.