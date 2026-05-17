#!/bin/sh
set -e

mkdir -p /output

keytool -genkey -v \
  -keystore /output/release.keystore \
  -alias "$ALIAS_NAME" \
  -keyalg RSA -keysize 2048 \
  -validity 10000 \
  -dname "CN=Unknown,OU=Unknown,O=Unknown,L=Unknown,ST=Unknown,C=US" \
  -storepass "$KEYSTORE_PASS" \
  -keypass "$ALIAS_PASS" \
  >/dev/null 2>&1

LOCAL_PROPERTIES=$(printf 'KEYSTORE_PASS=%s\nALIAS_NAME=%s\nALIAS_PASS=%s\n' \
  "$KEYSTORE_PASS" "$ALIAS_NAME" "$ALIAS_PASS" \
  | base64 | tr -d '\n')

RELEASE_KEYSTORE=$(base64 /output/release.keystore | tr -d '\n')

echo "=== SECRET: LOCAL_PROPERTIES ==="
echo "$LOCAL_PROPERTIES"
echo ""
echo "=== SECRET: RELEASE_KEYSTORE ==="
echo "$RELEASE_KEYSTORE"
