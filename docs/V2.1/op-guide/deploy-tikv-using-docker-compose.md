---
title: Install and Deploy TiKV Using Docker Compose
summary: Use Docker Compose to quickly deploy a TiKV testing cluster on a single machine.
category: operations
---

# Install and Deploy TiKV Using Docker Compose

This guide describes how to quickly deploy a TiKV testing cluster using [Docker Compose](https://github.com/pingcap/tidb-docker-compose/) on a single machine. Currently, this installation method only supports the Linux system.

> **Warning:** Do not use Docker Compose to deploy the TiKV cluster in the production environment. For production, [use Ansible to deploy the TiKV cluster](deploy-tikv-using-ansible.md).

## Prerequisites

Make sure you have installed the following items on your machine:

- Docker (17.06.0 or later) and Docker Compose

    ```bash
    sudo yum install docker docker-compose
    ```

- Git

    ```
    sudo yum install git
    ```

## Install 

Download `tidb-docker-compose`.

```bash
git clone https://github.com/pingcap/tidb-docker-compose.git
```

## Prepare cluster

In this example, let's run a simple cluster with only 1 PD server and 1 TiKV server. See the following for the basic docker compose configuration file:

```yaml
version: '2.1'

services:
  pd0:
    image: pingcap/pd:latest
    ports:
      - "2379"
    volumes:
      - ./config/pd.toml:/pd.toml:ro
      - ./data:/data
      - ./logs:/logs
    command:
      - --name=pd0
      - --client-urls=http://0.0.0.0:2379
      - --peer-urls=http://0.0.0.0:2380
      - --advertise-client-urls=http://pd0:2379
      - --advertise-peer-urls=http://pd0:2380
      - --initial-cluster=pd0=http://pd0:2380
      - --data-dir=/data/pd0
      - --config=/pd.toml
      - --log-file=/logs/pd0.log
    restart: on-failure
  
  tikv0:
    image: pingcap/tikv:latest
    volumes:
      - ./config/tikv.toml:/tikv.toml:ro
      - ./data:/data
      - ./logs:/logs
    command:
      - --addr=0.0.0.0:20160
      - --advertise-addr=tikv0:20160
      - --data-dir=/data/tikv0
      - --pd=pd0:2379
      - --config=/tikv.toml
      - --log-file=/logs/tikv0.log
    depends_on:
      - "pd0"
    restart: on-failure
```

All the following example docker compose config file will contain this base config.

## Example

### Run [YCSB](https://github.com/pingcap/go-ycsb) to connect to the TiKV cluster:

1. Create a `ycsb-docker-compose.yml` file, add the above base config to this file, and then append the following section:

    ```yaml
    ycsb:
      image: pingcap/go-ycsb
    ```

2. Start the cluster:

    ```bash
    rm -rf data logs 
    docker-compose -f ycsb-docker-compose.yml pull 
    docker-compose -f ycsb-docker-compose.yml up -d
    ```

3. Start YCSB:

    ```bash
    docker-compose -f ycsb-docker-compose.yml run ycsb shell tikv -p tikv.pd=pd0:2379
    ```

4. Run YCSB:

    ```bash
    INFO[0000] [pd] create pd client with endpoints [pd0:2379]
    INFO[0000] [pd] leader switches to: http://pd0:2379, previous:
    INFO[0000] [pd] init cluster id 6628733331417096653
    » read a
    Read empty for a
    » insert a field0=0
    Insert a ok
    » read a
    Read a ok
    field0="0"
    ```

### Use [Titan](https://github.com/meitu/titan) to connect to the TiKV cluster via the Redis protocol:

1. Create a `titan-docker-compose.yml` file, add the above base config to this file, and then append the following section:

    ```yaml
      titan:
        image: meitu/titan
        ports:
          - "7369:7369"
        command:
          - --pd-addrs=tikv://pd0:2379
        depends_on:
          - "tikv0"
        restart: on-failure    
    ```

2. Start the cluster:

    ```bash
    rm -rf data logs 
    docker-compose -f titan-docker-compose.yml pull
    docker-compose -f titan-docker-compose.yml up -d
    ```

3. Use `redis-cli` to communicate with Titan:

    ```bash
    redis-cli -p 7369
    127.0.0.1:7369> set a 1
    OK
    127.0.0.1:7369> get a
    "1"
    ```

## What's next?

+ If you want to try the Go client, see [Try Two Types of APIs](../clients/go-client-api.md). You need to build your docker image and add it to the docker compose config file like above YCSB or Titan does. 
+ If you want to run a full cluster with monitor support, please follow the [tidb-docker-compose guide](https://github.com/pingcap/tidb-docker-compose/blob/master/README.md), comment the `tidb` and `tispark` sections out in the [values.yaml](https://github.com/pingcap/tidb-docker-compose/blob/master/compose/values.yaml), generate the new docker compose config, then add your own binary image and run it.
