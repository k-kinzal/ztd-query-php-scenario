#!/bin/sh
set -eu
if [ "$SQLITE_VERSION" = system ]; then
    exit 0
fi
: "${SQLITE_URL:?A source URL is required for a custom SQLite version}"
: "${SQLITE_SHA256:?A SHA-256 is required for a custom SQLite version}"
curl --fail --show-error --location --retry 3 "$SQLITE_URL" -o /tmp/sqlite.tar.gz
printf '%s  /tmp/sqlite.tar.gz\n' "$SQLITE_SHA256" | sha256sum --check --strict
mkdir /tmp/sqlite-src
tar -xzf /tmp/sqlite.tar.gz -C /tmp/sqlite-src --strip-components=1
cd /tmp/sqlite-src
CFLAGS='-O2 -DSQLITE_ENABLE_COLUMN_METADATA' ./configure \
    --prefix=/usr/local --enable-shared --disable-static --enable-fts5
# Only PHP needs the library; avoid compiling a second copy into the CLI.
make -j1 libsqlite3.la
make install-libLTLIBRARIES install-includeHEADERS install-pkgconfigDATA
printf '/usr/local/lib\n' > /etc/ld.so.conf.d/00-sqlite.conf
ldconfig
rm -rf /tmp/sqlite-src /tmp/sqlite.tar.gz
