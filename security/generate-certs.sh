#!/bin/bash

cd /work

openssl req -new -x509 -keyout ca.key -out ca.crt -days 365 -passout pass:sentinel -subj "/C=IT/O=KafkaSentinel/CN=SentinelCA"

for i in broker-1 broker-2 broker-3 client; do
  mkdir -p ${i}-creds
  
  # Generazione della chiave privata
  openssl genrsa -out ${i}-creds/${i}.key 2048
  
  # Generazione della richiesta di firma (CSR)
  openssl req -new -key ${i}-creds/${i}.key -out ${i}-creds/${i}.csr -subj "/C=IT/O=KafkaSentinel/CN=${i}"
  
  # firma del certificato
  openssl x509 -req -in ${i}-creds/${i}.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out ${i}-creds/${i}.crt -days 365 -passin pass:sentinel
  
done

# Pulizia dei file temporanei
rm *.csr 2>/dev/null || true
rm *.srl 2>/dev/null || true

echo "Certificati TLS pronti"