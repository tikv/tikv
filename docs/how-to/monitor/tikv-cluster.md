---
title: Monitor a TiKV Cluster
summary: Learn how to monitor the state of a TiKV cluster.
category: how-to
---

# Monitor a TiKV Cluster

Currently, you can use two types of interfaces to monitor the state of the TiKV cluster:

- [The component state interface](#the-component-state-interface): use the HTTP interface to get the internal information of a component, which is called the component state interface.
- [The metrics interface](#the-metrics-interface): use the Prometheus interface to record the detailed information of various operations in the components, which is called the metrics interface.

## The component state interface

You can use this type of interface to monitor the basic information of components. This interface can get the details of the entire TiKV cluster and can act as the interface to monitor Keepalive.

### The PD server

The API address of the Placement Driver (PD) is `http://${host}:${port}/pd/api/v1/${api_name}`

The default port number is 2379.

For detailed information about various API names, see [PD API doc](https://download.pingcap.com/pd-api-v1.html).

You can use the interface to get the state of all the TiKV instances and the information about load balancing. It is the most important and frequently-used interface to get the state information of all the TiKV nodes. See the following example for the information about a 3-instance TiKV cluster deployed on a single machine:

```bash
curl http://127.0.0.1:2379/pd/api/v1/stores
{
  "count": 3,
  "stores": [
    {
      "store": {
        "id": 1,
        "address": "127.0.0.1:20161",
        "version": "2.1.0-rc.2",
        "state_name": "Up"
      },
      "status": {
        "capacity": "937 GiB",
        "available": "837 GiB",
        "leader_weight": 1,
        "region_count": 1,
        "region_weight": 1,
        "region_score": 1,
        "region_size": 1,
        "start_ts": "2018-09-29T00:05:47Z",
        "last_heartbeat_ts": "2018-09-29T00:23:46.227350716Z",
        "uptime": "17m59.227350716s"
      }
    },
    {
      "store": {
        "id": 2,
        "address": "127.0.0.1:20162",
        "version": "2.1.0-rc.2",
        "state_name": "Up"
      },
      "status": {
        "capacity": "937 GiB",
        "available": "837 GiB",
        "leader_weight": 1,
        "region_count": 1,
        "region_weight": 1,
        "region_score": 1,
        "region_size": 1,
        "start_ts": "2018-09-29T00:05:47Z",
        "last_heartbeat_ts": "2018-09-29T00:23:45.65292648Z",
        "uptime": "17m58.65292648s"
      }
    },
    {
      "store": {
        "id": 7,
        "address": "127.0.0.1:20160",
        "version": "2.1.0-rc.2",
        "state_name": "Up"
      },
      "status": {
        "capacity": "937 GiB",
        "available": "837 GiB",
        "leader_count": 1,
        "leader_weight": 1,
        "leader_score": 1,
        "leader_size": 1,
        "region_count": 1,
        "region_weight": 1,
        "region_score": 1,
        "region_size": 1,
        "start_ts": "2018-09-29T00:05:47Z",
        "last_heartbeat_ts": "2018-09-29T00:23:44.853636067Z",
        "uptime": "17m57.853636067s"
      }
    }
  ]
}
```

## The metrics interface

You can use this type of interface to monitor the state and performance of the entire cluster. The metrics data is displayed in Prometheus and Grafana. See [Use Prometheus and Grafana](#use-prometheus-and-grafana) for how to set up the monitoring system.

You can get the following metrics for each component:

### The PD server

- the total number of times that the command executes
- the total number of times that a certain command fails
- the duration that a command succeeds
- the duration that a command fails
- the duration that a command finishes and returns result

### The TiKV server

- Garbage Collection (GC) monitoring
- the total number of times that the TiKV command executes
- the duration that Scheduler executes commands
- the total number of times of the Raft propose command
- the duration that Raft executes commands
- the total number of times that Raft commands fail
- the total number of times that Raft processes the ready state

## Use Prometheus and Grafana

This section introduces the deployment architecture of Prometheus and Grafana in TiKV, and how to set up and configure the monitoring system.

### The deployment architecture

See the following diagram for the deployment architecture:

![deployment architecture of Prometheus and Grafana in TiKV](../../images/monitor-architecture.png)

> **Note:** You must add the Prometheus Pushgateway addresses to the startup parameters of the PD and TiKV components.

### Set up the monitoring system

See the following links for your reference:

- Prometheus Pushgateway: [https://github.com/prometheus/pushgateway](https://github.com/prometheus/pushgateway)

- Prometheus Server: [https://github.com/prometheus/prometheus#install](https://github.com/prometheus/prometheus#install)

- Grafana: [http://docs.grafana.org](http://docs.grafana.org/)

## Manual configuration

This section describes how to manually configure PD and TiKV, PushServer, Prometheus, and Grafana.

> **Note:** If your TiKV cluster is deployed using Ansible or Docker Compose, the configuration is automatically done, and generally, you do not need to configure it manually again. If your TiKV cluster is deployed using Docker, you can follow the configuration steps below.

### Configure PD and TiKV

+ PD: update the `toml` configuration file with the Pushgateway address and the the push frequency:

    ```toml
    [metric]
    # prometheus client push interval, set "0s" to disable prometheus.
    interval = "15s"
    # prometheus pushgateway address, leaves it empty will disable prometheus.
    address = "host:port"
    ```

+ TiKV: update the `toml` configuration file with the Pushgateway address and the the push frequency. Set the `job` field to `"tikv"`.

    ```toml
    [metric]
    # the Prometheus client push interval. Setting the value to 0s stops Prometheus client from pushing.
    interval = "15s"
    # the Prometheus pushgateway address. Leaving it empty stops Prometheus client from pushing.
    address = "host:port"
    # the Prometheus client push job name. Note: A node id will automatically append, e.g., "tikv_1".
    job = "tikv"
    ```

### Configure PushServer

Generally, you can use the default port `9091` and do not need to configure PushServer.

### Configure Prometheus

Add the Pushgateway address to the `yaml` configuration file:

```yaml
 scrape_configs:
# The job name is added as a label `job=<job_name>` to any timeseries scraped from this config.
- job_name: 'TiKV'

  # Override the global default and scrape targets from this job every 5 seconds.
  scrape_interval: 5s

  honor_labels: true

  static_configs:
 - targets: ['host:port'] # use the Pushgateway address
labels:
  group: 'production'
 ```

### Configure Grafana

#### Create a Prometheus data source

1. Log in to the Grafana Web interface.

    - Default address: [http://localhost:3000](http://localhost:3000)
    - Default account name: admin
    - Default password: admin

2. Click the Grafana logo to open the sidebar menu.

3. Click "Data Sources" in the sidebar.

4. Click "Add data source".

5. Specify the data source information:

    - Specify the name for the data source.
    - For Type, select Prometheus.
    - For Url, specify the Prometheus address.
    - Specify other fields as needed.

6. Click "Add" to save the new data source.

#### Create a Grafana dashboard

1. Click the Grafana logo to open the sidebar menu.

2. On the sidebar menu, click "Dashboards" -> "Import" to open the "Import Dashboard" window.

3. Click "Upload .json File" to upload a JSON file (Download [TiDB Grafana Config](https://grafana.com/tidb)).

4. Click "Save & Open".

5. A Prometheus dashboard is created.