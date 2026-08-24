#!/bin/bash
# Configura i permessi ACL per producer e consumer.
# Esegui una sola volta dopo "docker compose up -d".
# Aspetta qualche secondo che i broker siano completamente avviati.

sleep 15

docker exec kafka-1 kafka-acls --bootstrap-server localhost:29092 \
  --add --allow-principal 'User:CN=producer' \
  --producer --topic test-transazioni

docker exec kafka-1 kafka-acls --bootstrap-server localhost:29092 \
  --add --allow-principal 'User:CN=consumer' \
  --consumer --topic test-transazioni \
  --group fraud-detection-group

echo "ACL configurate."
