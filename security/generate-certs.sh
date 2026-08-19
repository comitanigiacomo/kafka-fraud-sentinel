#!/bin/bash
cd /work
openssl req -new -x509 -keyout ca.key -out ca.crt -days 365 -passout pass:sentinel -subj "/C=IT/O=KafkaSentinel/CN=SentinelCA"

openssl pkcs12 -export -in ca.crt -nokeys -out truststore.p12 -passout pass:sentinel
echo "sentinel" > truststore.password

for i in broker-1 broker-2 broker-3 client; do
  mkdir -p ${i}-creds
  openssl genrsa -out ${i}-creds/${i}.key 2048
  openssl req -new -key ${i}-creds/${i}.key -out ${i}-creds/${i}.csr -subj "/C=IT/O=KafkaSentinel/CN=${i}"
  openssl x509 -req -in ${i}-creds/${i}.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out ${i}-creds/${i}.crt -days 365 -passin pass:sentinel
  
  openssl pkcs12 -export -in ${i}-creds/${i}.crt -inkey ${i}-creds/${i}.key -out ${i}-creds/${i}.keystore.p12 -name ${i} -passout pass:sentinel
  
  echo "sentinel" > ${i}-creds/${i}.keystore.password
  
  cp truststore.p12 ${i}-creds/
  cp truststore.password ${i}-creds/
done

rm *.csr *.srl 2>/dev/null || true
echo "Certificati pronti."