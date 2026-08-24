#!/bin/bash
# Configura i permessi ACL per producer e consumer.
# Esegui una sola volta dopo "docker compose up -d".
# Aspetta che i broker siano completamente avviati.

echo "Aspetto che Kafka sia pronto..."
sleep 30

docker exec kafka-1 kafka-acls --bootstrap-server localhost:29092 \
  --add --allow-principal 'User:CN=producer' \
  --producer --topic test-transazioni 2>/dev/null

docker exec kafka-1 kafka-acls --bootstrap-server localhost:29092 \
  --add --allow-principal 'User:CN=consumer' \
  --consumer --topic test-transazioni \
  --group fraud-detection-group 2>/dev/null

echo "ACL configurate."
