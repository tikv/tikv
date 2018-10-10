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

- Helm

    ```bash
    curl https://raw.githubusercontent.com/kubernetes/helm/master/scripts/get | bash
    ```

- Git

    ```
    sudo yum install git
    ```

## Install and deploy

1. Download `tidb-docker-compose`.

    ```bash
    git clone https://github.com/pingcap/tidb-docker-compose.git
    ```

2. Edit the `compose/values.yaml` file to configure `networkMode` to `host`.

    ```bash
    cd tidb-docker-compose
    vim compose/values.yaml  
    ```

3. Edit the `compose/values.yaml` file to comment the TiDB section out.

4. Change the Prometheus and Pushgateway addresses for the `host` network mode.

    ```bash
    sed -i 's/pushgateway:9091/127.0.0.1:9091/g' config/*
    sed -i 's/prometheus:9090/127.0.0.1:9090/g' config/*
    ```

5. Generate the `generated-docker-compose.yml` file.

    ```bash
    helm template compose > generated-docker-compose.yml
    ```

6. Create and start the cluster using the `generated-docker-compose.yml` file.

    ```bash
    docker-compose -f generated-docker-compose.yml pull # Get the latest Docker images
    docker-compose -f generated-docker-compose.yml up -d
    ```

You can check whether the TiKV cluster has been successfully deployed using the following command:

```bash
curl localhost:2379/pd/api/v1/stores
```

If the state of all the TiKV instances is "Up", you have successfully deployed a TiKV cluster.

## What's next?

If you want to try the Go client, see [Try Two Types of APIs](../clients/go-client-api.md).