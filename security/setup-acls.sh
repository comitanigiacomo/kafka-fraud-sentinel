#!/bin/bash
# Configura i permessi ACL per producer, consumer e dashboard.
# Esegui una sola volta dopo "docker compose up -d".

echo "Aspetto che Kafka sia pronto..."
sleep 30

# Permessi per il producer: può scrivere sul topic principale
docker exec kafka-1 kafka-acls --bootstrap-server localhost:29092 \
  --add --allow-principal 'User:CN=producer' \
  --producer --topic test-transazioni 2>/dev/null

# Permessi per il consumer: può leggere dal topic principale
docker exec kafka-1 kafka-acls --bootstrap-server localhost:29092 \
  --add --allow-principal 'User:CN=consumer' \
  --consumer --topic test-transazioni \
  --group fraud-detection-group 2>/dev/null

# Il consumer pubblica anche gli alert su fraud-alerts, quindi ha bisogno
# anche dei permessi di scrittura su quel topic
docker exec kafka-1 kafka-acls --bootstrap-server localhost:29092 \
  --add --allow-principal 'User:CN=consumer' \
  --producer --topic fraud-alerts 2>/dev/null

# La dashboard usa il certificato admin per leggere dal topic fraud-alerts
# con un consumer group separato (dashboard-group)
docker exec kafka-1 kafka-acls --bootstrap-server localhost:29092 \
  --add --allow-principal 'User:CN=consumer' \
  --consumer --topic fraud-alerts \
  --group dashboard-group 2>/dev/null

echo "ACL configurate."
