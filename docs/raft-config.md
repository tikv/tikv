---
title: Raft Configuration
summary: Learn how to configure Raft.
category: adopters
---

# Raft Configuration

Raft is the distributed consensus algorithm which backs TiKV. This document describes how to configure Raft in TiKV.

To ensure configuration options are always up to date, refer to the `etc/config-template.toml` file. All options are documented there.

## `tikv-ctl raft`

You can use `tikv-ctl` to issue some commands to the Raft store. To get any information you might need about using any command, use the `--help` command.

Currently, you can issue the following two commands related to Raft via `tikv-ctl`.

### `tikv-ctl raft region`

> **Note:** Currently, the Region command is work in progress [WIP], both with or without `-r`.

This command outputs information regarding Regions in TiKV.

- To list all Regions with information:

    ```bash
    tikv-ctl raft region
    ```

- To print out information about a specific region:

    ```bash
    # Region 1
    tikv-ctl raft region -r 1
    ```

The output information for a region looks like this:

```bash
TODO: These commands are unimplemented.
```

### `tikv-ctl raft log`

> **Note:** Currently, the log command is partially implemented.

This command outputs some information about a raw entry in the Raft history at a certain index for a given Region:

```bash
# Region 1, index 3
tikv-ctl --host $PD_HOST raft log -r 1 -i 3
```

This will log something like:

```
idx_key: \001\002\000\000\000\000\000\000\000\001\001\000\000\000\000\000\000\000\003
region: 1
log index: 3
DebugClient::raft_log: RpcFailure(RpcStatus { status: Unimplemented, details: Some("unknown service debugpb.Debug") })
```