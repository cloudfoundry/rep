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

certstrap init --common-name "root-ca" --passphrase ""
certstrap request-cert --common-name "intermed-ca" --passphrase "" --ip "127.0.0.1"
certstrap sign intermed-ca --CA "root-ca" --intermediate
certstrap request-cert --common-name "server" --passphrase "" --ip "127.0.0.1"
certstrap sign server --CA "intermed-ca"
cat out/server.crt out/intermed-ca.crt > ./chain-certs/chain.crt
cat out/server.crt > ./chain-certs/bad-chain.crt && tail -n5 out/intermed-ca.crt >> ./chain-certs/bad-chain.crt

mv -f out/* ./chain-certs/
rm -rf out

popd
