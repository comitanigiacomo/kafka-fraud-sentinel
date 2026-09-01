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
        print(f"Errore di invio transazione: {err}")
    else:
        print(f"Transazione inoltrata -> Topic: {msg.topic()} | Partizione: {msg.partition()} | Offset: {msg.offset()}")

# Configura il producer con mTLS: CA per validare il broker, certificato client per autenticarsi
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
# Genera una transazione casuale, con un 15% di possibilità di avere un importo alto (sospetto)
def generate_mock_transaction():
    users = [f"user_{i}" for i in range(100, 200)]
    merchants = ["Amazon", "Supermarket Roma", "ATM Milano", "CryptoExchange", "Luxury Store", "Tech Shop"]
    
    is_high_amount = random.random() < 0.30 
    
    transaction = {
        "transaction_id": f"tx_{random.randint(100000, 999999)}",
        "user_id": random.choice(users),
        "amount": round(random.uniform(1000.0, 8500.0) if is_high_amount else random.uniform(5.0, 150.0), 2),
        "currency": "EUR",
        "merchant": random.choice(merchants),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
    return transaction

def main():
    print("Avvio del servizio Producer (Simulatore Transazioni)...")
    
    users = [f"user_{i}" for i in range(100, 200)]
    
    try:
        producer = Producer(get_kafka_config())
    except Exception as e:
        print(f"Errore critico di connessione al cluster Kafka: {e}")
        return

    topic_name = "transactions"

    try:
        counter = 1
        while True:
            # Con il 15% di probabilità, genero una "raffica" per simulare un velocity fraud naturale
            is_velocity_burst = random.random() < 0.15

            if is_velocity_burst:
                burst_user = random.choice(users)
                burst_size = random.randint(4, 6)
                print(f"--> Inizio raffica di {burst_size} transazioni per {burst_user} (Velocity Fraud)...")
                for _ in range(burst_size):
                    tx = generate_mock_transaction()
                    tx["user_id"] = burst_user  # Forza lo stesso utente
                    tx["amount"] = round(random.uniform(5.0, 50.0), 2)  # Importi piccoli per non confondersi con amount fraud
                    
                    try:
                        payload = json.dumps(tx)
                    except (TypeError, ValueError) as e:
                        continue

                    producer.produce(topic=topic_name, key=tx["user_id"].encode('utf-8'), value=payload.encode('utf-8'), callback=delivery_report)
                    producer.poll(0)
                    print(f"  -> Inviata transazione #{counter}: {tx['user_id']} ha speso {tx['amount']} {tx['currency']}")
                    counter += 1
                    time.sleep(0.2)
                print(f"--> Fine raffica per {burst_user}")
            
            else:
                # Transazione singola normale (che può includere Amount Fraud)
                tx = generate_mock_transaction()
                try:
                    payload = json.dumps(tx)
                except (TypeError, ValueError) as e:
                    continue
                
                producer.produce(topic=topic_name, key=tx["user_id"].encode('utf-8'), value=payload.encode('utf-8'), callback=delivery_report)
                producer.poll(0)
                
                print(f"-> Inviata transazione #{counter}: {tx['user_id']} ha speso {tx['amount']} {tx['currency']} presso {tx['merchant']}")
                counter += 1

            time.sleep(random.uniform(1, 2))
            
    except KeyboardInterrupt:
        print("\n Interruzione ricevuta. Svuotamento della coda in corso...")
        producer.flush(timeout=10)
        print("Producer terminato.")

if __name__ == '__main__':
    main()