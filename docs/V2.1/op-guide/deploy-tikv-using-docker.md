---
title: Install and Deploy TiKV Using Docker
summary: Use Docker to deploy a TiKV cluster on multiple nodes.
category: operations
---

# Install and Deploy TiKV Using Docker

This guide describes how to deploy a multi-node TiKV cluster using Docker.

> **Warning:** Do not use Docker to deploy the TiKV cluster in the production environment. For production, [use Ansible to deploy the TiKV cluster](deploy-tikv-using-ansible.md).

## Prerequisites

Make sure that Docker is installed on each machine.

For more details about prerequisites, see [Hardware and Software Requirements](https://github.com/pingcap/docs/blob/master/op-guide/recommendation.md).

## Deploy the TiKV cluster on multiple nodes

Assume that you have 6 machines with the following details:

| Name      | Host IP       | Services   | Data Path |
| --------- | ------------- | ---------- | --------- |
| Node1     | 192.168.1.101 | PD1        | /data     |
| Node2     | 192.168.1.102 | PD2        | /data     |
| Node3     | 192.168.1.103 | PD3        | /data     |
| Node4     | 192.168.1.104 | TiKV1      | /data     |
| Node5     | 192.168.1.105 | TiKV2      | /data     |
| Node6     | 192.168.1.106 | TiKV3      | /data     |

If you want to test TiKV with a limited number of nodes, you can also use one PD instance to test the entire cluster.

### Step 1: Pull the latest images of TiKV and PD from Docker Hub

Start Docker and pull the latest images of TiKV and PD from [Docker Hub](https://hub.docker.com) using the following command:

```bash
docker pull pingcap/tikv:latest
docker pull pingcap/pd:latest
```

### Step 2: Log in and start PD

Log in to the three PD machines and start PD respectively:

1. Start PD1 on Node1:

    ```bash
    docker run -d --name pd1 \
    -p 2379:2379 \
    -p 2380:2380 \
    -v /etc/localtime:/etc/localtime:ro \
    -v /data:/data \
    pingcap/pd:latest \
    --name="pd1" \
    --data-dir="/data/pd1" \
    --client-urls="http://0.0.0.0:2379" \
    --advertise-client-urls="http://192.168.1.101:2379" \
    --peer-urls="http://0.0.0.0:2380" \
    --advertise-peer-urls="http://192.168.1.101:2380" \
    --initial-cluster="pd1=http://192.168.1.101:2380,pd2=http://192.168.1.102:2380,pd3=http://192.168.1.103:2380"
    ```

2. Start PD2 on Node2:

    ```bash
    docker run -d --name pd2 \
    -p 2379:2379 \
    -p 2380:2380 \
    -v /etc/localtime:/etc/localtime:ro \
    -v /data:/data \
    pingcap/pd:latest \
    --name="pd2" \
    --data-dir="/data/pd2" \
    --client-urls="http://0.0.0.0:2379" \
    --advertise-client-urls="http://192.168.1.102:2379" \
    --peer-urls="http://0.0.0.0:2380" \
    --advertise-peer-urls="http://192.168.1.102:2380" \
    --initial-cluster="pd1=http://192.168.1.101:2380,pd2=http://192.168.1.102:2380,pd3=http://192.168.1.103:2380"
    ```

3. Start PD3 on Node3:

    ```bash
    docker run -d --name pd3 \
    -p 2379:2379 \
    -p 2380:2380 \
    -v /etc/localtime:/etc/localtime:ro \
    -v /data:/data \
    pingcap/pd:latest \
    --name="pd3" \
    --data-dir="/data/pd3" \
    --client-urls="http://0.0.0.0:2379" \
    --advertise-client-urls="http://192.168.1.103:2379" \
    --peer-urls="http://0.0.0.0:2380" \
    --advertise-peer-urls="http://192.168.1.103:2380" \
    --initial-cluster="pd1=http://192.168.1.101:2380,pd2=http://192.168.1.102:2380,pd3=http://192.168.1.103:2380"
    ```

### Step 3: Log in and start TiKV

Log in to the three TiKV machines and start TiKV respectively:

1. Start TiKV1 on Node4:

    ```bash
    docker run -d --name tikv1 \
    -p 20160:20160 \
    -v /etc/localtime:/etc/localtime:ro \
    -v /data:/data \
    pingcap/tikv:latest \
    --addr="0.0.0.0:20160" \
    --advertise-addr="192.168.1.104:20160" \
    --data-dir="/data/tikv1" \
    --pd="192.168.1.101:2379,192.168.1.102:2379,192.168.1.103:2379"
    ```

2. Start TiKV2 on Node5:

    ```bash
    docker run -d --name tikv2 \
    -p 20160:20160 \
    -v /etc/localtime:/etc/localtime:ro \
    -v /data:/data \
    pingcap/tikv:latest \
    --addr="0.0.0.0:20160" \
    --advertise-addr="192.168.1.105:20160" \
    --data-dir="/data/tikv2" \
    --pd="192.168.1.101:2379,192.168.1.102:2379,192.168.1.103:2379"
    ```

3. Start TiKV3 on Node6:

    ```bash
    docker run -d --name tikv3 \
    -p 20160:20160 \
    -v /etc/localtime:/etc/localtime:ro \
    -v /data:/data \
    pingcap/tikv:latest \
    --addr="0.0.0.0:20160" \
    --advertise-addr="192.168.1.106:20160" \
    --data-dir="/data/tikv3" \
    --pd="192.168.1.101:2379,192.168.1.102:2379,192.168.1.103:2379"
    ```

You can check whether the TiKV cluster has been successfully deployed using the following command:

```
curl 192.168.1.101:2379/pd/api/v1/stores
```

If the state of all the TiKV instances is "Up", you have successfully deployed a TiKV cluster.

## What's next?

If you want to try the Go client, see [Try Two Types of APIs](../clients/go-client-api.md).