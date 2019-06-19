---
title: Scale a TiKV Cluster
summary: Learn how to scale out or scale in a TiKV cluster.
category: how-to
---

# Scale a TiKV Cluster

You can scale out a TiKV cluster by adding nodes to increase the capacity without affecting online services. You can also scale in a TiKV cluster by deleting nodes to decrease the capacity without affecting online services.

> **Note:** If your TiKV cluster is deployed using Ansible, see [Scale the TiKV Cluster Using TiDB-Ansible](ansible-deployment-scale.md).

## Scale out or scale in PD

Before increasing or decreasing the capacity of PD, you can view details of the current PD cluster. Assume you have three PD servers with the following details:

| Name | ClientUrls        | PeerUrls          |
|:-----|:------------------|:------------------|
| pd1  | http://host1:2379 | http://host1:2380 |
| pd2  | http://host2:2379 | http://host2:2380 |
| pd3  | http://host3:2379 | http://host3:2380 |

Get the information about the existing PD nodes through `pd-ctl`:

```bash
./pd-ctl -u http://host1:2379
>> member
```

For the usage of `pd-ctl`, see [PD Control User Guide](../../reference/tools/pd-control.md).

### Add a PD node dynamically

You can add a new PD node to the current PD cluster using the `join` parameter. To add `pd4`, use `--join` to specify the client URL of any PD server in the PD cluster, like:

```bash
./bin/pd-server --name=pd4 \
                --client-urls="http://host4:2379" \
                --peer-urls="http://host4:2380" \
                --join="http://host1:2379"
```

### Remove a PD node dynamically

You can remove `pd4` using `pd-ctl`:

```bash
./pd-ctl -u http://host1:2379
>> member delete pd4
```

### Replace a PD node dynamically

You might want to replace a PD node in the following scenarios:

- You need to replace a faulty PD node with a healthy PD node.
- You need to replace a healthy PD node with a different PD node.

To replace a PD node, first add a new node to the cluster, migrate all the data from the node you want to remove, and then remove the node.

You can only replace one PD node at a time. If you want to replace multiple nodes, repeat the above steps until you have replaced all nodes. After completing each step, you can verify the process by checking the information of all nodes.

## Scale out or scale in TiKV

Before increasing or decreasing the capacity of TiKV, you can view details of the current TiKV cluster. Get the information about the existing TiKV nodes through `pd-ctl`:

```bash
./pd-ctl -u http://host1:2379
>> store
```

### Add a TiKV node dynamically

To add a new TiKV node dynamically, start a TiKV node on a new machine. The newly started TiKV node will automatically register in the existing PD of the cluster.

To reduce the pressure of the existing TiKV nodes, PD loads balance automatically, which means PD gradually migrates some data to the new TiKV node.

### Remove a TiKV node dynamically

To remove a TiKV node safely, you need to inform PD in advance. After that, PD is able to migrate the data on this TiKV node to other TiKV nodes, ensuring that data have enough replicas.

For example, to remove the TiKV node with the store id 1, you can complete this using `pd-ctl`:

```bash
./pd-ctl -u http://host1:2379
>> store delete 1
```

Then you can check the state of this TiKV node:

```bash
./pd-ctl -u http://host1:2379
>> store 1
{
  "store": {
    "id": 1,
    "address": "127.0.0.1:21060",
    "state": 1,
    "state_name": "Offline"
  },
  "status": {
    ...
  }
}
```

You can verify the state of this store using `state_name`:

  - `state_name=Up`: This store is in service.
  - `state_name=Disconnected`: The heartbeats of this store cannot be detected currently, which might be caused by a failure or network interruption.
  - `state_name=Down`: PD does not receive heartbeats from the TiKV store for more than an hour (the time can be configured using `max-down-time`). At this time, PD adds a replica for the data on this store.
  - `state_name=Offline`: This store is shutting down, but the store is still in service.
  - `state_name=Tombstone`: This store is shut down and has no data on it, so the instance can be removed.

### Replace a TiKV node dynamically

You might want to replace a TiKV node in the following scenarios:

- You need to replace a faulty TiKV node with a healthy TiKV node.
- You need to replace a healthy TiKV node with a different TiKV node.

To replace a TiKV node, first add a new node to the cluster, migrate all the data from the node you want to remove, and then remove the node.

You can only replace one TiKV node at a time. To verify whether a node has been made offline, you can check the state information of the node in process. After verifying, you can make the next node offline.