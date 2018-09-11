---
title: Scale a TiKV Cluster
summary: Learn how to scale out or scale in a TiKV cluster.
category: operations
---

# Scale a TiKV Cluster

You can scale out a TiKV cluster by adding nodes to increase the capacity without affecting online services. You can also scale in a TiKV cluster by deleting nodes to decrease the capacity without affecting online services.

> **Note:** If your TiKV cluster is deployed using Ansible, see [Scale the TiKV Cluster Using TiDB-Ansible](ansible-deployment-scale.md).

## Scale PD

Before scaling the capacity of PD, you can view details of the current PD cluster. Assume you have three PD servers with the following details:

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

For the usage of `pd-ctl`, see [PD Control User Guide](../tools/pd-control.md).

### Add a PD node dynamically

You can add a new PD node to the current PD cluster using the `join` parameter. To add `pd4`, use `--join` to specify the client URL of any PD server in the PD cluster, like:

```bash
./bin/pd-server --name=pd4 \
                --client-urls="http://host4:2379" \
                --peer-urls="http://host4:2380" \
                --join="http://host1:2379"
```

### Delete a PD node dynamically

You can delete `pd4` using `pd-ctl`:

```bash
./pd-ctl -u http://host1:2379
>> member delete pd4
```

### Migrate a PD node dynamically

If you want to migrate a node to a new machine, first add a node on the new machine, and then delete the node on the old machine.

You can only migrate one node at a time. If you want to migrate multiple nodes, repeat the above steps until you have migrated all nodes. After completing each step, you can verify the process by checking the information of all nodes.

## Scale TiKV

Get the information about the existing TiKV nodes through `pd-ctl`:

```bash
./pd-ctl -u http://host1:2379
>> store
```

### Add a TiKV node dynamically

To add a new TiKV node dynamically, start a TiKV node on a new machine. The newly started TiKV node will automatically register in the existing PD of the cluster.

To reduce the pressure of the existing TiKV nodes, PD loads balance automatically, which means PD gradually migrates some data to the new TiKV node.

### Delete a TiKV node dynamically

To delete a TiKV node safely, you need to inform PD in advance. After that, PD is able to migrate the data on this TiKV node to other TiKV nodes, ensuring that data have enough replicas.

For example, to delete the TiKV node with the store id 1, you can complete this using `pd-ctl`:

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
  - `state_name=Tombstone`: This store is shut down and has no data on it, so the instance can be deleted.

### Migrate a TiKV node dynamically

To migrate a TiKV node to a new machine, first add a node on the new machine, and then make all nodes on the old machine offline.

In the process of migration, you can add all machines in the new cluster to the existing cluster, and then make old nodes offline one by one.

To verify whether a node has been made offline, you can check the state information of the node in process. After verifying, you can make the next node offline.
