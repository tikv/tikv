---
title: Secure the TiKV Cluster 
summary: Learn how to secure the TiKV cluster.
category: operations
---

# Secure the TiKV Cluster

TiKV has SSL/TLS integration to encrypt the data exchanged between nodes. This document describes how to secure the TiKV cluster.

## ca-path = "/path/to/ca.pem"

The path to the file that contains the PEM encoding of the server’s CA certificates.

## cert-path = "/path/to/cert.pem"

The path to the file that contains the PEM encoding of the server’s certificate chain.

## key-path = "/path/to/key.pem"

The path to the file that contains the PEM encoding of the server’s private key.