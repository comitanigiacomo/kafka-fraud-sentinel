#!/bin/bash
# Configura i permessi ACL per producer, consumer e dashboard.
# Esegui una sola volta dopo "docker compose up -d".

echo "Aspetto che Kafka sia pronto..."
sleep 30

# Permessi per il producer: può scrivere sul topic transactions
docker exec kafka-1 kafka-acls --bootstrap-server localhost:29092 \
  --add --allow-principal 'User:CN=producer' \
  --producer --topic transactions 2>/dev/null

# Permessi per il consumer: può leggere dal topic transactions e dal suo gruppo
docker exec kafka-1 kafka-acls --bootstrap-server localhost:29092 \
  --add --allow-principal 'User:CN=consumer' \
  --consumer --topic transactions \
  --group fraud-detection-group 2>/dev/null

# Il consumer pubblica anche gli alert su fraud-alerts, quindi ha bisogno
# anche dei permessi di scrittura su quel topic
docker exec kafka-1 kafka-acls --bootstrap-server localhost:29092 \
  --add --allow-principal 'User:CN=consumer' \
  --producer --topic fraud-alerts 2>/dev/null

# La dashboard si connette con il certificato admin (admin.crt), quindi il suo principal
# e' CN=admin. Gli diamo i permessi per leggere da fraud-alerts con il suo consumer group.
docker exec kafka-1 kafka-acls --bootstrap-server localhost:29092 \
  --add --allow-principal 'User:CN=admin' \
  --consumer --topic fraud-alerts \
  --group dashboard-group 2>/dev/null

# Permessi per AKHQ (si connette senza certificato, quindi il suo principal e' ANONYMOUS).
# Invece di renderlo super-user (che bypassa tutto), gli diamo solo
# i permessi di lettura necessari per il monitoraggio del cluster.
docker exec kafka-1 kafka-acls --bootstrap-server localhost:29092 \
  --add --allow-principal 'User:ANONYMOUS' \
  --operation Describe --operation DescribeConfigs \
  --topic '*' 2>/dev/null

docker exec kafka-1 kafka-acls --bootstrap-server localhost:29092 \
  --add --allow-principal 'User:ANONYMOUS' \
  --operation Describe \
  --group '*' 2>/dev/null

docker exec kafka-1 kafka-acls --bootstrap-server localhost:29092 \
  --add --allow-principal 'User:ANONYMOUS' \
  --operation Read \
  --topic '*' 2>/dev/null

docker exec kafka-1 kafka-acls --bootstrap-server localhost:29092 \
  --add --allow-principal 'User:ANONYMOUS' \
  --operation Read \
  --group '*' 2>/dev/null

echo "ACL configurate."
