---
title: TiKV Security Configuration 
summary: Learn about the security configuration in TiKV.
category: reference
---

# TiKV Security Configuration

TiKV has SSL/TLS integration to encrypt the data exchanged between nodes. This document describes the security configuration in the TiKV cluster.

## ca-path = "/path/to/ca.pem"

The path to the file that contains the PEM encoding of the server’s CA certificates.

## cert-path = "/path/to/cert.pem"

The path to the file that contains the PEM encoding of the server’s certificate chain.

## key-path = "/path/to/key.pem"

The path to the file that contains the PEM encoding of the server’s private key.