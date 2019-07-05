---
title: Label Configuration
summary: Learn how to configure labels.
category: how-to
---

# Label Configuration

TiKV uses labels to label its location information and PD schedulers according to the topology of the cluster, to maximize TiKV's capability of disaster recovery. This document describes how to configure labels.

## TiKV reports the topological information

In order for PD to get the topology of the cluster, TiKV reports the topological information to PD according to the startup parameter or configuration of TiKV. Assume that the topology has three structures: zone > rack > host, use labels to specify the following information for each TiKV:

- Startup parameter:

    ```
    tikv-server --labels zone=<zone>,rack=<rack>,host=<host>
    ```

- Configuration:

    ```
    [server]
    labels = "zone=<zone>,rack=<rack>,host=<host>"
    ```
## PD understands the TiKV topology

After getting the topology of the TiKV cluster, PD also needs to know the hierarchical relationship of the topology. You can configure it through the PD configuration or `pd-ctl`:

- PD configuration:

    ```
    [replication]
    max-replicas = 3
    location-labels = ["zone", "rack", "host"]
    ```

- PD controller:

    ```
    pd-ctl >> config set location-labels zone,rack,host
    ```

To make PD understand that the labels represents the TiKV topology, keep `location-labels` corresponding to the TiKV `labels` name. See the following example.

### Example

PD makes optimal scheduling according to the topological information. You just need to care about what kind of topology can achieve the desired effect.

If you use 3 replicas and hope that the TiDB cluster is always highly available even when a data zone goes down, you need at least 4 data zones.

Assume that you have 4 data zones, each zone has 2 racks, and each rack has 2 hosts. You can start 2 TiKV instances on each host as follows:

Startup TiKV:

```
# zone=z1
tikv-server --labels zone=z1,rack=r1,host=h1
tikv-server --labels zone=z1,rack=r1,host=h2
tikv-server --labels zone=z1,rack=r2,host=h1
tikv-server --labels zone=z1,rack=r2,host=h2

# zone=z2
tikv-server --labels zone=z2,rack=r1,host=h1
tikv-server --labels zone=z2,rack=r1,host=h2
tikv-server --labels zone=z2,rack=r2,host=h1
tikv-server --labels zone=z2,rack=r2,host=h2

# zone=z3
tikv-server --labels zone=z3,rack=r1,host=h1
tikv-server --labels zone=z3,rack=r1,host=h2
tikv-server --labels zone=z3,rack=r2,host=h1
tikv-server --labels zone=z3,rack=r2,host=h2

# zone=z4
tikv-server --labels zone=z4,rack=r1,host=h1
tikv-server --labels zone=z4,rack=r1,host=h2
tikv-server --labels zone=z4,rack=r2,host=h1
tikv-server --labels zone=z4,rack=r2,host=h2
```

Configure PD:

```
use `pd-ctl` connect the PD:
# pd-ctl 
>> config set location-labels zone,rack,host
```

Now the cluster can work well. 16 TiKV instances are distributed across 4 data zones, 8 racks and 16 machines. In this case, PD schedules different replicas of each datum to different data zones.

- If one of the data zones goes down, the high availability of the TiDB cluster is not affected.
- If the data zone cannot recover within a period of time, PD will remove the replica from this data zone.

PD maximizes the disaster recovery of the cluster according to the current topology. Therefore, if you want to reach a certain level of disaster recovery, deploy many machines in different sites according to the topology. The number of machines must be more than the number of `max-replicas`.