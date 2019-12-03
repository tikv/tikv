cat <<-EOF
Package: tikv
Version: ${TIKV_BUILD_VERSION}
Section: databases
Priority: optional
Architecture: amd64
Depends: systemd
Maintainer: The TiKV Authors
Description:
 A distributed transactional key-value store.
EOF