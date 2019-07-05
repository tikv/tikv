---
title: Overview of the TiKV Monitoring Framework
summary: Use Prometheus and Grafana to build the TiKV monitoring framework.
category: operations
---

# Overview of the TiKV Monitoring Framework

The TiKV monitoring framework adopts two open-source projects: [Prometheus](https://github.com/prometheus/prometheus) and [Grafana](https://github.com/grafana/grafana). TiKV uses Prometheus to store the monitoring and performance metrics, and uses Grafana to visualize these metrics.

## About Prometheus in TiKV

As a time series database, Prometheus has a multi-dimensional data model and flexible query language. Prometheus consists of multiple components. Currently, TiKV uses the following components:

- Prometheus Server: to scrape and store time series data
- Client libraries: to customize necessary metrics in the application
- Pushgateway: to receive the data from Client Push for the Prometheus main server
- AlertManager: for the alerting mechanism

The diagram is as follows:

![Prometheus in TiKV](../../images/prometheus-in-tikv.png)

## About Grafana in TiKV

[Grafana](https://github.com/grafana/grafana) is an open-source project for analyzing and visualizing metrics. TiKV uses Grafana to display the performance metrics.