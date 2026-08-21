#!/bin/bash
cd /work
rm -rf *creds *.p12 *.password *.crt *.key *.pem *.cnf *.jks *.csr 2>/dev/null || true

keytool -genkeypair -alias ca -keyalg RSA -keysize 2048 -dname "CN=SentinelCA" -ext bc:c=ca:true -validity 365 -keystore ca.jks -storepass sentinel -keypass sentinel -storetype JKS

keytool -exportcert -alias ca -keystore ca.jks -storepass sentinel -rfc -file ca.pem

for i in 1 2 3; do
  DIR="broker-${i}-creds"
  mkdir -p ${DIR}

  keytool -genkeypair -alias broker -keyalg RSA -keysize 2048 -dname "CN=localhost" -ext "san=dns:localhost,dns:kafka-${i},ip:127.0.0.1" -validity 365 -keystore ${DIR}/broker-${i}.keystore.jks -storepass sentinel -keypass sentinel -storetype JKS
  keytool -certreq -alias broker -keystore ${DIR}/broker-${i}.keystore.jks -storepass sentinel -file ${DIR}/broker.csr
  keytool -gencert -alias ca -keystore ca.jks -storepass sentinel -infile ${DIR}/broker.csr -outfile ${DIR}/broker.crt -ext "san=dns:localhost,dns:kafka-${i},ip:127.0.0.1" -validity 365

  keytool -importcert -alias ca -keystore ${DIR}/broker-${i}.keystore.jks -storepass sentinel -file ca.pem -noprompt
  keytool -importcert -alias broker -keystore ${DIR}/broker-${i}.keystore.jks -storepass sentinel -file ${DIR}/broker.crt -noprompt
  
done

echo "Certificati pronti."