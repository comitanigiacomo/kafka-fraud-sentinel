#!/bin/bash
# Configura i permessi ACL per producer, consumer e il tool di monitoraggio AKHQ.
# Esegui una sola volta dopo "docker compose up -d".

echo "Aspetto che Kafka sia pronto..."
sleep 30

# Permessi per il producer: può scrivere sul topic transactions
docker exec kafka-1 kafka-acls --bootstrap-server localhost:29092 \
  --add --allow-principal 'User:CN=producer' \
  --producer --topic transactions 2>/dev/null

# Permessi per il consumer: può leggere dal topic e dal suo consumer group
docker exec kafka-1 kafka-acls --bootstrap-server localhost:29092 \
  --add --allow-principal 'User:CN=consumer' \
  --consumer --topic transactions \
  --group fraud-detection-group 2>/dev/null

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
