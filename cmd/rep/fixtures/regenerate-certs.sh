#!/bin/bash

set -e

this_dir="$(cd $(dirname $0) && pwd)"

pushd "$this_dir"

certstrap init --common-name "server-ca" --passphrase ""
certstrap request-cert --common-name "client" --passphrase "" --ip "127.0.0.1"
certstrap sign client --CA "server-ca"

certstrap request-cert --common-name "server" --passphrase "" --ip "127.0.0.1" --domain "*.bbs.service.cf.internal"
certstrap sign server --CA "server-ca"

mv -f out/* ./blue-certs/
rm -rf out

popd
