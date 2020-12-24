#!/usr/bin/env bash
#
# Generate self-signed certs for the various TiKV components and clients.
# self-signed are usefult for testing and cannot be used for production.
#
#     mkdir certs
#     cd certs
#     cert_dir="$(pwd)"
#     CERT_PATH="${cert_dir}" ../scripts/certs-generate
#     cd ..
#     pushd "$GOPATH/src/github.com/tikv/pd"
#     ./scripts/run-tls --config "${cert_dir}/pd-certs.toml" &
#     popd
#     ./scripts/run-tls --config certs/tikv-certs.toml
#
set -euo pipefail

# Useful to create an absolute path for the cert
# Does not have a trailing slash
CERT_PATH=${CERT_PATH:-""}
if [[ -n $CERT_PATH ]] ; then
	CERT_PATH="${CERT_PATH}/"
fi

if ! [[ -e openssl.cnf ]] ; then
    cp "$(find / -name openssl.cnf -print -quit 2>/dev/null)" .
    # echo ".include filename"
    cat <<-EOF >> openssl.cnf

[ req ]
req_extensions = v3_req

[ v3_req ]
subjectAltName = @alt_names
EOF

    cat <<-EOF >> openssl.cnf

[ alt_names ]
IP.1 = 127.0.0.1
IP.2 = 172.16.10.14
IP.3 = 172.16.10.15
IP.4 = 172.16.10.16
EOF
fi

if ! [[ -e root.key ]] ; then
    echo "generate root.key"
    openssl genrsa -des3 -out root.key 4096
fi
if ! [[ -e root.crt ]] ; then
    echo "generate root.crt"
    openssl req -new -x509 -days 1000 -key root.key -out root.crt -subj '/CN=localhost'
    # validate
    openssl x509 -text -in root.crt -noout >/dev/null
fi

components=(tikv tidb pd)
for component in "${components[@]}" ; do
    if ! [[ -e ${component}.key ]] ; then
        echo "gen ${component}.key"
        openssl genrsa -out "${component}.key" 2048
    fi
    if ! [[ -e ${component}.crt ]] ; then
        echo "gen ${component}.crt"
        openssl req -new -key ${component}.key -out ${component}.csr -config openssl.cnf -subj "/CN=${component}"
        openssl x509 -req -days 365 -CA root.crt -CAkey root.key -CAcreateserial -in ${component}.csr -out ${component}.crt -extensions v3_req -extfile openssl.cnf
        # validate
        openssl x509 -text -in "${component}.crt" -noout >/dev/null
    fi

    # All of our components use slightly different names
    if [[ $component == tikv ]] ; then
cat <<-EOF > "${component}-certs.toml"
[security]
ca-path = "${CERT_PATH}root.crt"
cert-path = "${CERT_PATH}${component}.crt"
key-path = "${CERT_PATH}${component}.key"
# cert-allowed-cn = ["${component}-ctl", "${component}", "tidb", "pd"]
EOF
fi

    if [[ $component == pd ]] ; then
cat <<-EOF > "${component}-certs.toml"
[security]
# Path of file that contains list of trusted SSL CAs. If set, following four settings shouldn't be empty
cacert-path = "${CERT_PATH}root.crt"
# Path of file that contains X509 certificate in PEM format.
cert-path = "${CERT_PATH}${component}.crt"
# Path of file that contains X509 key in PEM format.
key-path = "${CERT_PATH}${component}.key"
EOF
fi

    if [[ $component == tidb ]] ; then
cat <<-EOF > "${component}-certs.toml"
[security]
# Path of file that contains list of trusted SSL CAs for connection with cluster components.
cluster-ssl-ca = "${CERT_PATH}root.crt"
# Path of file that contains X509 certificate in PEM format for connection with cluster components.
cluster-ssl-cert = "${CERT_PATH}${component}.crt"
# Path of file that contains X509 key in PEM format for connection with cluster components.
cluster-ssl-key = "${CERT_PATH}${component}.key"
EOF
fi

    ctl="${component}-ctl"
    if ! [[ -e $ctl.key ]] ; then
        echo "gen ${ctl}.key"
        openssl genrsa -out "${ctl}.key" 2048
    fi
    if ! [[ -e ${ctl}.csr ]] ; then
        echo "gen ${ctl}.csr"
        openssl req -new -key "${ctl}.key" -out "${ctl}.csr" -subj "/CN=${ctl}"
    fi
    if ! [[ -e ${ctl}.crt ]] ; then
        echo "gen ${ctl}.crt"
        openssl x509 -req -days 365 -CA "root.crt" -CAkey "root.key" -CAcreateserial -in "${ctl}.csr" -out ${ctl}.crt
    fi
done
