import os
import time
import json
import random
from datetime import datetime, timezone
from dotenv import load_dotenv
from confluent_kafka import Producer

basedir = os.path.abspath(os.path.dirname(__file__))
load_dotenv(os.path.join(basedir, '../../.env'))

def delivery_report(err, msg):
    if err is not None:
        print(f"Errore di invio: {err}")
    else:
        print(f"Inviato -> Partizione: {msg.partition()} | Offset: {msg.offset()}")

def get_kafka_config():
    return {
        'bootstrap.servers': 'localhost:9092,localhost:9094,localhost:9095',
        'security.protocol': 'SSL',
        'ssl.ca.location': os.path.join(basedir, '../../security/ca.pem'),
        'ssl.certificate.location': os.path.join(basedir, '../../security/producer.crt'),
        'ssl.key.location': os.path.join(basedir, '../../security/producer.key.pem'),
        'ssl.endpoint.identification.algorithm': 'none',
        'acks': 'all',
        'retries': 3
    }

def main():
    # Simula un attacco di tipo "velocity fraud": manda una raffica di transazioni
    # per lo stesso utente in pochissimo tempo, cosi' il consumer le individua
    # come sospette grazie al controllo sulla finestra temporale.

    print("Avvio stress producer - simulazione velocity fraud...")

    try:
        producer = Producer(get_kafka_config())
    except Exception as e:
        print(f"Errore di connessione: {e}")
        return

    # L'utente che verra' segnalato come sospetto
    target_user = "user_101"
    # Numero di transazioni da mandare in burst (ben oltre la soglia di 3 in 10 secondi)
    burst_count = 15
    merchants = ["Amazon", "CryptoExchange", "ATM Milano", "Luxury Store"]

    print(f"Invio {burst_count} transazioni per {target_user} in rapida successione...")

    for i in range(burst_count):
        tx = {
            "transaction_id": f"stress_{random.randint(100000, 999999)}",
            "user_id": target_user,
            "amount": round(random.uniform(20.0, 200.0), 2),
            "currency": "EUR",
            "merchant": random.choice(merchants),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

        producer.produce(
            topic="transactions",
            key=target_user.encode('utf-8'),
            value=json.dumps(tx).encode('utf-8'),
            callback=delivery_report
        )
        producer.poll(0)
        print(f"  [{i+1}/{burst_count}] {tx['amount']} EUR presso {tx['merchant']}")

        # Pausa minima tra un messaggio e l'altro
        time.sleep(0.3)

    print("\nBurst completato. Svuoto la coda...")
    producer.flush(timeout=10)
    print("Stress producer terminato.")

if __name__ == '__main__':
    main()
