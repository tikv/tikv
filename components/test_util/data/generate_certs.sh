#!/bin/bash
#
# Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

set -euo pipefail

SCRIPT_PATH="$(realpath "$0")"
CERT_DIR="$(dirname "$SCRIPT_PATH")"
CA_KEY="$CERT_DIR/ca.key"
CA_CERT="$CERT_DIR/ca.pem"
SERVER_KEY="$CERT_DIR/key.pem"
SERVER_CSR="$CERT_DIR/server.csr"
SERVER_CERT="$CERT_DIR/server.pem"
VALID_DAYS=3650
RSA_KEY_SIZE=2048

# CA certs.
openssl genrsa -out "$CA_KEY" "$RSA_KEY_SIZE"
openssl req -new -x509 -days "$VALID_DAYS" -key "$CA_KEY" -out "$CA_CERT" \
    -subj "/CN=tikv_test_ca" \
    -addext "basicConstraints = critical,CA:TRUE,pathlen:0" \
    -addext "keyUsage = cRLSign, keyCertSign"
echo "CA certificate:"
openssl x509 -text -in "$CA_CERT" -noout

# Server certs.
openssl genrsa -out "$SERVER_KEY" "$RSA_KEY_SIZE"
openssl req -new -key "$SERVER_KEY" -out "$SERVER_CSR" \
    -extensions v3_ca \
    -subj "/CN=tikv-server" \
    -addext "basicConstraints = critical, CA:FALSE" \
    -addext "keyUsage = critical, digitalSignature, keyEncipherment" \
    -addext "extendedKeyUsage = serverAuth, clientAuth" \
    -addext "subjectAltName = IP.1:172.16.5.40, IP.2:127.0.0.1"
openssl x509 -req -days "$VALID_DAYS" \
    -CA "$CA_CERT" -CAkey "$CA_KEY" -CAcreateserial \
    -copy_extensions copyall \
    -in "$SERVER_CSR" -out "$SERVER_CERT"
echo "Server certificate:"
openssl x509 -text -in "$SERVER_CERT" -noout
