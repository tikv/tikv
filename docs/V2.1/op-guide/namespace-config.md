---
title: Namespace Configuration
summary: Learn how to configure namespace in TiKV.
category: operations
---

# Namespace Configuration

Namespace is a mechanism used to meet the requirements of resource isolation. In this mechanism, TiKV supports dividing all the TiKV nodes in the cluster among multiple separate namespaces and classifying Regions into the corresponding namespace by using a custom namespace classifier.

In this case, there is actually a constraint for the scheduling policy: the namespace that a Region belongs to should match the namespace of TiKV where each replica of this Region resides. PD continuously performs the constraint check during runtime. When it finds unmatched namespaces, it will schedule the Regions to make the replica distribution conform to the namespace configuration.

In a typical TiDB cluster, the most common resource isolation requirement is resource isolation based on the SQL table schema -- for example, using non-overlapping hosts to carry data for different services. Therefore, PD provides `tableNamespaceClassifier` based on the SQL table schema by default. You can also adjust the PD server's `--namespace-classifier` parameter to use another custom classifier.

## Configure namespace

You can use `pd-ctl` to configure the namespace based on the table schema. All related operations are integrated in the `table_ns` subcommand. Here is an example.

1. Create 2 namespaces:

    ```bash
    ./bin/area-pd-ctl
    » table_ns
    {
        "count": 0,
        "namespaces": []
    }

    » table_ns create ns1
    » table_ns create ns2
    » table_ns
    {
        "count": 2,
        "namespaces": [
            {
            "ID": 30,
            "Name": "ns1"
            },
            {
            "ID": 31,
            "Name": "ns2"
            }
        ]
    }
    ```

Then two namespaces, `ns1` and `ns2`, are created. But they do not work because they are not bound with any TiKV nodes or tables.

2. Divide some TiKV nodes to the 2 namespaces:

    ```bash
    » table_ns set_store 1 ns1
    » table_ns set_store 2 ns1
    » table_ns set_store 3 ns1
    » table_ns set_store 4 ns2
    » table_ns set_store 5 ns2
    » table_ns set_store 6 ns2
    » table_ns
    {
        "count": 2,
        "namespaces": [
            {
            "ID": 30,
            "Name": "ns1",
            "store_ids": {
                "1": true,
                "2": true,
                "3": true
            }
            },
            {
            "ID": 31,
            "Name": "ns2",
            "store_ids": {
                "4": true,
                "5": true,
                "6": true
            }
            }
        ]
    }
    ```

3. Divide some tables to the corresponding namespace (the table ID information can be obtained through TiDB's API):

    ```bash
    » table_ns add ns1 1001
    » table_ns add ns2 1002
    » table_ns
    {
        "count": 2,
        "namespaces": [
            {
            "ID": 30,
            "Name": "ns1",
            "table_ids": {
                "1001": true
            },
            "store_ids": {
                "1": true,
                "2": true,
                "3": true
            }
            },
            {
            "ID": 31,
            "Name": "ns2",
            "table_ids": {
                "1002": true
            },
            "store_ids": {
                "4": true,
                "5": true,
                "6": true
            }
            }
        ]
    }
    ```

The namespace configuration is finished. PD will schedule the replicas of table 1001 to TiKV nodes 1,2, and 3 and schedule the replicas of table 1002 to TiKV nodes 4, 5, and 6.

In addition, PD supports some other `table_ns` subcommands, such as the `remove` and `rm_store` commands which remove the table and TiKV node from the specified namespace respectively. PD also supports setting different scheduling configurations within the namespace. For more details, see [PD Control User Guide](https://github.com/pingcap/docs/blob/master/tools/pd-control.md).

When the namespace configuration is updated, the namespace constraint may be violated. It will take a while for PD to complete the scheduling process. You can view all Regions that violate the constraint using the `pd-ctl` command `region check incorrect-ns`.