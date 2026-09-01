#!/bin/bash
# Simula il crash di un broker mentre il cluster e' attivo.
# Serve per dimostrare la fault tolerance: il sistema non si interrompe
# anche se un nodo cade, e quando torna online si riallinea da solo.

echo "=== Demo Fault Tolerance ==="
echo ""
echo "Verifico che i 3 broker siano attivi..."
docker ps --format "{{.Names}}" | grep kafka
echo ""

TARGET="kafka-2"

echo "Fermo il broker $TARGET..."
docker stop $TARGET
echo "-> $TARGET stoppato. Il cluster ora ha solo 2 nodi su 3."
echo "   Controlla il terminale del producer: deve continuare a inviare."
echo ""

echo "Aspetto 15 secondi per osservare il comportamento..."
sleep 15

echo ""
echo "Riavvio $TARGET..."
docker start $TARGET
echo "-> $TARGET riavviato. Si riallinea con il leader e recupera i messaggi persi."
echo ""

echo "Aspetto che $TARGET si riallinei (circa 10 secondi)..."
sleep 10

echo ""
echo "=== Demo completata ==="
echo "Il cluster ha continuato a funzionare con 2 broker su 3."
echo "Questo e' possibile perche' replication.factor=3 e min.insync.replicas=2,"
echo "quindi il cluster tollera la perdita di 1 nodo senza perdere dati."
