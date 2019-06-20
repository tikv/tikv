---
title: gRPC Configuration 
summary: Learn how to configure gRPC.
category: operations
---

# gRPC Configuration

TiKV uses gRPC, a remote procedure call (RPC) framework, to build a distributed transactional key-value database. gRPC is designed to be high-performance, but ill-configured gRPC leads to performance regression of TiKV. This document describes how to configure gRPC.

## grpc-compression-type

- Compression type for the gRPC channel
- Default: "none"
- Available values are “none”, “deflate” and “gzip” 
- To exchange the CPU time for network I/O, you can set it to “deflate” or “gzip”. It is useful when the network bandwidth is limited

## grpc-concurrency

- The size of the thread pool that drives gRPC
- Default: 4. It is suitable for a commodity computer. You can double the size if TiKV is deployed in a high-end server (32 core+ CPU)
- Higher concurrency is for higher QPS, but it consumes more CPU

## grpc-concurrent-stream

- The number of max concurrent streams/requests on a connection
- Default: 1024. It is suitable for most workload
- Increase the number if you find that most of your requests are not time consuming, e.g., RawKV Get

## grpc-keepalive-time

- Time to wait before sending out a ping to check whether the server is still alive. This is only for the communication between TiKV instances
- Default: 10s

## grpc-keepalive-timeout

- Time to wait before closing the connection without receiving the `keepalive` ping ACK
- Default: 3s

## grpc-raft-conn-num

- The number of connections with each TiKV server to send Raft messages
- Default: 10

## grpc-stream-initial-window-size

- Amount to Read Ahead on individual gRPC streams
- Default: 2MB
- Larger values can help throughput on high-latency connections