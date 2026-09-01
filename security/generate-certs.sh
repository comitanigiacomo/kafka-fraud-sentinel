#!/bin/bash
cd "$(dirname "$0")"

# Genera la CA (Certificate Authority) interna del progetto
openssl genrsa -out ca.key 2048
openssl req -x509 -new -nodes -key ca.key -sha256 -days 365 -out ca.pem -subj "/CN=SentinelCA"

# Genera un certificato per ogni broker Kafka
for i in 1 2 3; do
  mkdir -p broker-${i}-creds

  openssl genrsa -out broker-${i}-creds/broker.key 2048
  openssl req -new -key broker-${i}-creds/broker.key \
    -out broker-${i}-creds/broker.csr \
    -subj "/CN=kafka-${i}"

  echo "subjectAltName=DNS:kafka-${i},DNS:localhost" > san.ext
  openssl x509 -req \
    -in broker-${i}-creds/broker.csr \
    -CA ca.pem -CAkey ca.key -CAcreateserial \
    -out broker-${i}-creds/broker.crt \
    -days 365 -sha256 \
    -extfile san.ext

  cat broker-${i}-creds/broker.crt broker-${i}-creds/broker.key > broker-${i}-creds/broker-keystore.pem

  cp ca.pem broker-${i}-creds/ca.pem
  rm -f broker-${i}-creds/broker.csr san.ext
done

# Genera un certificato client per producer, consumer e admin
for CLIENT in producer consumer admin; do
  openssl genrsa -out ${CLIENT}.key.pem 2048
  openssl req -new -key ${CLIENT}.key.pem -out ${CLIENT}.csr -subj "/CN=${CLIENT}"
  openssl x509 -req -in ${CLIENT}.csr -CA ca.pem -CAkey ca.key -CAcreateserial -out ${CLIENT}.crt -days 365 -sha256
  rm -f ${CLIENT}.csr
done

echo "Certificati generati."