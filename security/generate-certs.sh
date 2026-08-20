#!/bin/bash
cd /work

openssl req -new -x509 -keyout ca.key -out ca.crt -days 365 -passout pass:sentinel -subj "/C=IT/O=KafkaSentinel/CN=SentinelCA"

keytool -importcert -trustcacerts -alias ca -file ca.crt -keystore truststore.p12 -storepass sentinel -noprompt -storetype PKCS12
echo -n "sentinel" > truststore.password

NODES=(
  "kafka-1 broker-1"
  "kafka-2 broker-2"
  "kafka-3 broker-3"
  "client client"
)

for NODE in "${NODES[@]}"; do
  set -- $NODE
  DNS=$1
  PREFIX=$2
  DIR="${PREFIX}-creds"
  
  mkdir -p ${DIR}
  
  openssl genrsa -out ${DIR}/${PREFIX}.key 2048
  openssl req -new -key ${DIR}/${PREFIX}.key -out ${DIR}/${PREFIX}.csr -subj "/C=IT/O=KafkaSentinel/CN=${DNS}"
  
  echo "subjectAltName=DNS:${DNS},DNS:localhost,IP:127.0.0.1" > ${DIR}/extfile.cnf
  
  openssl x509 -req -in ${DIR}/${PREFIX}.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out ${DIR}/${PREFIX}.crt -days 365 -passin pass:sentinel -extfile ${DIR}/extfile.cnf
  
  openssl pkcs12 -export -in ${DIR}/${PREFIX}.crt -inkey ${DIR}/${PREFIX}.key -certfile ca.crt -out ${DIR}/${PREFIX}.keystore.p12 -name ${PREFIX} -passout pass:sentinel
  
  echo -n "sentinel" > ${DIR}/${PREFIX}.keystore.password
  cp truststore.p12 ${DIR}/
  cp truststore.password ${DIR}/
  
  echo "Completato ${DNS}"
done

rm *.csr *.srl */*.cnf 2>/dev/null || true