import os
import json
import time
from datetime import datetime
from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaError

basedir = os.path.abspath(os.path.dirname(__file__))
load_dotenv(os.path.join(basedir, '../../.env'))

def get_kafka_config():
    return {
        'bootstrap.servers': 'localhost:9092,localhost:9094,localhost:9095',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': os.getenv('KAFKA_CLIENT_USER'),
        'sasl.password': os.getenv('KAFKA_CLIENT_PASSWORD'),
        'ssl.ca.location': os.path.join(basedir, '../../security/ca.pem'),
        'group.id': 'fraud-detection-group',
        'auto.offset.reset': 'earliest'
    }

def main():    
    consumer = Consumer(get_kafka_config())
    consumer.subscribe(['test-transazioni'])

    # Dizionario per tracciare la cronologia degli utenti in memoria
    # Formato: { user_id: [timestamp_1, timestamp_2, ...] }
    user_activity_history = {}

    TIME_WINDOW_SECONDS = 10  # Finestra temporale di controllo
    MAX_TRANSACTIONS_ALLOWED = 3 # Numero massimo di transazioni consentite nella finestra

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None: 
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF: 
                    continue
                else: 
                    print(f"[-] Errore Consumer: {msg.error()}")
                    break

            # Decodifico la transazione in arrivo dal topic
            tx = json.loads(msg.value().decode('utf-8'))
            user_id = tx['user_id']
            amount = tx['amount']
            
            # Converto la stringa ISO del timestamp in un timestamp numerico (epoch)
            tx_time = datetime.fromisoformat(tx['timestamp']).timestamp()

            print(f"[x] Analisi: {user_id} -> {amount} EUR")

            if user_id not in user_activity_history:
                user_activity_history[user_id] = []

            # 1. Aggiungo il timestamp attuale alla lista dell'utente
            user_activity_history[user_id].append(tx_time)

            # 2. Filtro tenendo solo le transazioni avvenute dentro la finestra temporale recente
            recent_txs = [t for t in user_activity_history[user_id] if (tx_time - t) <= TIME_WINDOW_SECONDS]
            user_activity_history[user_id] = recent_txs

            # 3. Controllo se superiamo la soglia di velocità o di importo anomalo
            is_velocity_fraud = len(recent_txs) > MAX_TRANSACTIONS_ALLOWED
            is_amount_fraud = amount > 5000.0

            if is_velocity_fraud:
                print(f"FRODE RILEVATA (Velocity Check): L'utente {user_id} ha fatto {len(recent_txs)} transazioni in meno di {TIME_WINDOW_SECONDS} secondi")
            
            if is_amount_fraud:
                print(f"FRODE RILEVATA (High Amount): Transazione sospetta di {amount} EUR per l'utente {user_id}!")

            if not is_velocity_fraud and not is_amount_fraud:
                print(f"-> Transazione regolare per {user_id}.")

    except KeyboardInterrupt:
        print("\n Interruzione ricevuta. Chiusura del consumer in corso...")
    finally:
        consumer.close()
        print("Consumer terminato correttamente.")

if __name__ == '__main__':
    main()