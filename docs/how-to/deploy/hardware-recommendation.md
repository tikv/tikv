---
title: Software and Hardware Requirements
summary: Learn the software and hardware requirements for deploying and running TiKV.
category: how-to
---

# Software and Hardware Requirements

As an open source distributed Key-Value database with high performance, TiKV can be deployed in the Intel architecture server and major virtualization environments and runs well. TiKV supports most of the major hardware networks and Linux operating systems.

TiKV must work together with [Placement Driver](https://github.com/pingcap/pd/) (PD). PD is the cluster manager of TiKV, which periodically checks replication constraints to balance load and data automatically.

## Linux OS version requirements

| Linux OS Platform        | Version      |
| :-----------------------:| :----------: |
| Red Hat Enterprise Linux | 7.3 or later |
| CentOS                   | 7.3 or later |
| Oracle Enterprise Linux  | 7.3 or later |
| Ubuntu LTS               | 16.04 or later |

> **Note:**
> 
> - For Oracle Enterprise Linux, TiKV supports the Red Hat Compatible Kernel (RHCK) and does not support the Unbreakable Enterprise Kernel provided by Oracle Enterprise Linux.
> - A large number of TiKV tests have been run on the CentOS 7.3 system, and in our community there are a lot of best practices in which TiKV is deployed on the Linux operating system. Therefore, it is recommended to deploy TiKV on CentOS 7.3 or later.
> - The support for the Linux operating systems above includes the deployment and operation in physical servers as well as in major virtualized environments like VMware, KVM and XEN.

## Server requirements

You can deploy and run TiKV on the 64-bit generic hardware server platform in the Intel x86-64 architecture. The requirements and recommendations about server hardware configuration for development, test and production environments are as follows:

### Development and test environments

| Component | CPU     | Memory | Local Storage  | Network  | Instance Number (Minimum Requirement) |
| :------: | :-----: | :-----: | :----------: | :------: | :----------------: |
| PD      | 4 core+   | 8 GB+  | SAS, 200 GB+ | Gigabit network card | 1    |
| TiKV    | 8 core+   | 32 GB+  | SAS, 200 GB+ | Gigabit network card | 3       |
|         |         |         |              | Total Server Number |  4      |

> **Note**: 
> 
> - Do not deploy PD and TiKV on the same server.
> - For performance-related test, do not use low-performance storage and network hardware configuration, in order to guarantee the correctness of the test result.

### Production environment

| Component | CPU | Memory | Hard Disk Type | Network | Instance Number (Minimum Requirement) |
| :-----: | :------: | :------: | :------: | :------: | :-----: |
| PD | 4 core+ | 8 GB+ | SSD | 10 Gigabit network card (2 preferred) | 3 |
| TiKV | 16 core+ | 32 GB+ | SSD | 10 Gigabit network card (2 preferred) | 3 |
| Monitor | 8 core+ | 16 GB+ | SAS | Gigabit network card | 1 |
|     |     |     |      |  Total Server Number   |    7   |

> **Note**:
> 
> - It is strongly recommended to use higher configuration in the production environment.
> - It is recommended to keep the size of TiKV hard disk within 2 TB if you are using PCI-E SSD disks or within 1.5 TB if you are using regular SSD disks.

## Network requirements

TiKV requires the following network port configuration to run. Based on the TiKV deployment in actual environments, the administrator can open relevant ports in the network side and host side.

| Component | Default Port | Description |
| :--:| :--: | :-- |
| TiKV | 20160 | the TiKV communication port |
| PD | 2380 | the inter-node communication port within the PD cluster |
| Pump | 8250 | the Pump communication port |
| Drainer | 8249 | the Drainer communication port |
| Prometheus | 9090 | the communication port for the Prometheus service|
| Pushgateway | 9091 | the aggregation and report port for TiKV, and PD monitor |
| Node_exporter | 9100 | the communication port to report the system information of each TiKV cluster node |
| Blackbox_exporter | 9115 | the `Blackbox_exporter` communication port, used to monitor the ports in the TiKV cluster |
| Grafana | 3000 | the port for the external Web monitoring service and client (Browser) access|
| Grafana | 8686 | the `grafana_collector` communication port, used to export the Dashboard as the PDF format |
| Kafka_exporter | 9308 | the `Kafka_exporter` communication port, used to monitor the binlog Kafka cluster |

## Web browser requirements

Based on the Prometheus and Grafana platform, TiKV provides a visual data monitoring solution to monitor the TiKV cluster status. To access the Grafana monitor interface, it is recommended to use a higher version of Microsoft IE, Google Chrome or Mozilla Firefox.
