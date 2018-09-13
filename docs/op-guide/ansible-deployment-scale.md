---
title: Scale a TiKV Cluster Using TiDB-Ansible
summary: Use TiDB-Ansible to scale out or scale in a TiKV cluster.
category: operations
---

# Scale a TiKV Cluster Using TiDB-Ansible

This document describes how to use TiDB-Ansible to scale out or scale in a TiKV cluster without affecting the online services.

> **Note:** This document applies to the TiKV deployment using Ansible. If your TiKV cluster is deployed in other ways, see [Scale a TiKV Cluster](horizontal-scale.md).

Assume that the topology is as follows:

| Name | Host IP | Services |
| ---- | ------- | -------- |
| node1 | 172.16.10.1 | PD1, Monitor |
| node2 | 172.16.10.2 | PD2 |
| node3 | 172.16.10.3 | PD3 |
| node4 | 172.16.10.4 | TiKV1 |
| node5 | 172.16.10.5 | TiKV2 |
| node6 | 172.16.10.6 | TiKV3 |

## Scale out a TiKV cluster

This section describes how to increase the capacity of a TiKV cluster by adding a TiKV or PD node.

### Add TiKV nodes

For example, if you want to add two TiKV nodes (node101, node102) with the IP addresses `172.16.10.101` and `172.16.10.102`, take the following steps:

1. Edit the `inventory.ini` file and append the TiKV node information in `tikv_servers`:

    ```ini
    [tidb_servers]

    [pd_servers]
    172.16.10.1
    172.16.10.2
    172.16.10.3

    [tikv_servers]
    172.16.10.4
    172.16.10.5
    172.16.10.6
    172.16.10.101
    172.16.10.102

    [monitoring_servers]
    172.16.10.1

    [grafana_servers]
    172.16.10.1

    [monitored_servers]
    172.16.10.1
    172.16.10.2
    172.16.10.3
    172.16.10.4
    172.16.10.5
    172.16.10.6
    172.16.10.101
    172.16.10.102
    ```

    Now the topology is as follows:

    | Name | Host IP | Services |
    | ---- | ------- | -------- |
    | node1 | 172.16.10.1 | PD1, Monitor |
    | node2 | 172.16.10.2 | PD2 |
    | node3 | 172.16.10.3 | PD3 |
    | node4 | 172.16.10.4 | TiKV1 |
    | node5 | 172.16.10.5 | TiKV2 |
    | node6 | 172.16.10.6 | TiKV3 |
    | **node101** | **172.16.10.101** | **TiKV4** |
    | **node102** | **172.16.10.102** | **TiKV5** |

2. Initialize the newly added node:

    ```bash
    ansible-playbook bootstrap.yml -l 172.16.10.101,172.16.10.102
    ```

    > **Note:** If an alias is configured in the `inventory.ini` file, for example, `node101 ansible_host=172.16.10.101`, use `-l` to specify the alias when executing `ansible-playbook`. For example, `ansible-playbook bootstrap.yml -l node101,node102`. This also applies to the following steps.

3. Deploy the newly added node:

    ```bash
    ansible-playbook deploy.yml -l 172.16.10.101,172.16.10.102
    ```

4. Start the newly added node:

    ```bash
    ansible-playbook start.yml -l 172.16.10.101,172.16.10.102
    ```

5. Update the Prometheus configuration and restart:

    ```bash
    ansible-playbook rolling_update_monitor.yml --tags=prometheus
    ```

6. Monitor the status of the entire cluster and the newly added nodes by opening a browser to access the monitoring platform: `http://172.16.10.1:3000`.

### Add a PD node

To add a PD node (node103) with the IP address `172.16.10.103`, take the following steps:

1. Edit the `inventory.ini` file and append the PD node information in `pd_servers`:

    ```ini
    [tidb_servers]

    [pd_servers]
    172.16.10.1
    172.16.10.2
    172.16.10.3
    172.16.10.103

    [tikv_servers]
    172.16.10.4
    172.16.10.5
    172.16.10.6

    [monitoring_servers]
    172.16.10.1

    [grafana_servers]
    172.16.10.1

    [monitored_servers]
    172.16.10.1
    172.16.10.2
    172.16.10.3
    172.16.10.103
    172.16.10.4
    172.16.10.5
    172.16.10.6
    ```
  
    Now the topology is as follows:
  
    | Name | Host IP | Services |
    | ---- | ------- | -------- |
    | node1 | 172.16.10.1 | PD1, Monitor |
    | node2 | 172.16.10.2 | PD2 |
    | node3 | 172.16.10.3 | PD3 |
    | **node103** | **172.16.10.103** | **PD4** |
    | node4 | 172.16.10.4 | TiKV1 |
    | node5 | 172.16.10.5 | TiKV2 |
    | node6 | 172.16.10.6 | TiKV3 |

2. Initialize the newly added node:

    ```bash
    ansible-playbook bootstrap.yml -l 172.16.10.103
    ```

3. Deploy the newly added node:

    ```bash
    ansible-playbook deploy.yml -l 172.16.10.103
    ```

4. Login the newly added PD node and edit the starting script:
  
    ```bash
    {deploy_dir}/scripts/run_pd.sh
    ```

    1. Remove the `--initial-cluster="xxxx" \` configuration.
    2. Add `--join="http://172.16.10.1:2379" \`. The IP address (`172.16.10.1`) can be any of the existing PD IP addresses in the cluster.
    3. Manually start the PD service in the newly added PD node:
      
        ```bash
        {deploy_dir}/scripts/start_pd.sh
        ```
      
    4. Use `pd-ctl` to check whether the new node is added successfully:
    
        ```bash
        ./pd-ctl -u "http://172.16.10.1:2379"
        ```
    
        > **Note:** `pd-ctl` is a command used to check the number of PD nodes.

5. Apply a rolling update to the entire cluster:
    
    ```bash
    ansible-playbook rolling_update.yml
    ```
   
6. Update the Prometheus configuration and restart:

    ```bash
    ansible-playbook rolling_update_monitor.yml --tags=prometheus
    ```

7. Monitor the status of the entire cluster and the newly added node by opening a browser to access the monitoring platform: `http://172.16.10.1:3000`.

## Scale in a TiKV cluster

This section describes how to decrease the capacity of a TiKV cluster by removing a TiKV or PD node.

> **Warning:** In decreasing the capacity, if your cluster has a mixed deployment of other services, do not perform the following procedures. The following examples assume that the removed nodes have no mixed deployment of other services.

### Remove a TiKV node

To remove a TiKV node (node6) with the IP address `172.16.10.6`, take the following steps:

1. Remove the node from the cluster using `pd-ctl`:

    1. View the store ID of node6:
        
        ```bash
        ./pd-ctl -u "http://172.16.10.1:2379" -d store
        ```

    2. Remove node6 from the cluster, assuming that the store ID is 10:
        
        ```bash
        ./pd-ctl -u "http://172.16.10.1:2379" -d store delete 10
        ```
        
2. Use Grafana or `pd-ctl` to check whether the node is successfully removed:

    ```bash
    ./pd-ctl -u "http://172.16.10.1:2379" -d store 10
    ```

    > **Note:** It takes some time to remove the node. If the status of the node you remove becomes Tombstone, then this node is successfully removed.

3. After the node is successfully removed, stop the services on node6:

    ```bash
    ansible-playbook stop.yml -l 172.16.10.6
    ```

4. Edit the `inventory.ini` file and remove the node information:

    ```ini
    [tidb_servers]

    [pd_servers]
    172.16.10.1
    172.16.10.2
    172.16.10.3

    [tikv_servers]
    172.16.10.4
    172.16.10.5
    #172.16.10.6  # the removed node

    [monitoring_servers]
    172.16.10.1

    [grafana_servers]
    172.16.10.1

    [monitored_servers]
    172.16.10.1
    172.16.10.2
    172.16.10.3
    172.16.10.4
    172.16.10.5
    #172.16.10.6  # the removed node
    ```

    Now the topology is as follows:

    | Name | Host IP | Services |
    | ---- | ------- | -------- |
    | node1 | 172.16.10.1 | PD1, Monitor |
    | node2 | 172.16.10.2 | PD2 |
    | node3 | 172.16.10.3 | PD3 |
    | node4 | 172.16.10.4 | TiKV1 |
    | node5 | 172.16.10.5 | TiKV2 |
    | **node6** | **172.16.10.6** | **TiKV3 removed** |

5. Update the Prometheus configuration and restart:

    ```bash
    ansible-playbook rolling_update_monitor.yml --tags=prometheus
    ```

6. Monitor the status of the entire cluster by opening a browser to access the monitoring platform: `http://172.16.10.1:3000`.

### Remove a PD node

To remove a PD node (node2) with the IP address `172.16.10.2`, take the following steps:

1. Remove the node from the cluster using `pd-ctl`:

    1. View the name of node2:

        ```bash
        ./pd-ctl -u "http://172.16.10.1:2379" -d member
        ```

    2. Remove node2 from the cluster, assuming that the name is pd2:

        ```bash
        ./pd-ctl -u "http://172.16.10.1:2379" -d member delete name pd2
        ```

2. Use Grafana or `pd-ctl` to check whether the node is successfully removed:

    ```bash
    ./pd-ctl -u "http://172.16.10.1:2379" -d member
    ```

3. After the node is successfully removed, stop the services on node2:

    ```bash
    ansible-playbook stop.yml -l 172.16.10.2
    ```

4. Edit the `inventory.ini` file and remove the node information:

    ```ini
    [tidb_servers]

    [pd_servers]
    172.16.10.1
    #172.16.10.2  # the removed node
    172.16.10.3

    [tikv_servers]
    172.16.10.4
    172.16.10.5
    172.16.10.6

    [monitoring_servers]
    172.16.10.1

    [grafana_servers]
    172.16.10.1

    [monitored_servers]
    172.16.10.1
    #172.16.10.2  # the removed node
    172.16.10.3
    172.16.10.4
    172.16.10.5
    172.16.10.6
    ```

    Now the topology is as follows:

    | Name | Host IP | Services |
    | ---- | ------- | -------- |
    | node1 | 172.16.10.1 | PD1, Monitor |
    | **node2** | **172.16.10.2** | **PD2 removed** |
    | node3 | 172.16.10.3 | PD3 |
    | node4 | 172.16.10.4 | TiKV1 |
    | node5 | 172.16.10.5 | TiKV2 |
    | node6 | 172.16.10.6 | TiKV3 |

5. Perform a rolling update to the entire TiKV cluster:

    ```bash
    ansible-playbook rolling_update.yml
    ```

6. Update the Prometheus configuration and restart:

    ```bash
    ansible-playbook rolling_update_monitor.yml --tags=prometheus
    ```

7. To monitor the status of the entire cluster, open a browser to access the monitoring platform: `http://172.16.10.1:3000`.