import os
import json
from datetime import datetime, timezone
from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaError
from pymongo import MongoClient

basedir = os.path.abspath(os.path.dirname(__file__))
load_dotenv(os.path.join(basedir, '../../.env'))

mongo_client = MongoClient(
    host="localhost",
    port=27017,
    username=os.getenv("MONGO_USER"),
    password=os.getenv("MONGO_PASSWORD")
)
db = mongo_client["fraud_sentinel"]
alerts_collection = db["alerts"]

def get_kafka_config():
    return {
        'bootstrap.servers': 'localhost:9092,localhost:9094,localhost:9095',
        'security.protocol': 'SSL',
        'ssl.ca.location': os.path.join(basedir, '../../security/ca.pem'),
        'ssl.certificate.location': os.path.join(basedir, '../../security/consumer.crt'),
        'ssl.key.location': os.path.join(basedir, '../../security/consumer.key.pem'),
        'ssl.endpoint.identification.algorithm': 'none',
        'group.id': 'fraud-detection-group',
        'auto.offset.reset': 'earliest'
    }

def main():    
    consumer = Consumer(get_kafka_config())
    consumer.subscribe(['transactions'])

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
                elif msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    # Il topic non esiste ancora, aspetto che il producer lo crei
                    continue
                else:
                    print(f"[-] Errore Consumer: {msg.error()}")
                    break

            # Decodifico la transazione gestendo eventuali messaggi corrotti
            try:
                tx = json.loads(msg.value().decode('utf-8'))
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue

            user_id = tx['user_id']
            amount = tx['amount']
            
            # Converto la stringa ISO del timestamp in un timestamp numerico
            tx_time = datetime.fromisoformat(tx['timestamp']).timestamp()

            print(f"Analisi: {user_id} -> {amount} EUR")

            if user_id not in user_activity_history:
                user_activity_history[user_id] = []

            # 1. Aggiungo il timestamp attuale alla lista dell'utente
            user_activity_history[user_id].append(tx_time)

            # 2. Filtro tenendo solo le transazioni avvenute dentro la finestra temporale recente
            recent_txs = [t for t in user_activity_history[user_id] if (tx_time - t) <= TIME_WINDOW_SECONDS]
            user_activity_history[user_id] = recent_txs

            # 3. Controllo superamento soglia di velocità o di importo anomalo
            is_velocity_fraud = len(recent_txs) > MAX_TRANSACTIONS_ALLOWED
            is_amount_fraud = amount > 5000.0

            # Raccolgo tutti gli alert in una lista per gestire il caso in cui
            # una transazione sia sospetta sia per importo che per velocita'
            alerts_to_save = []

            if is_velocity_fraud:
                print(f"(Velocity Check): L'utente {user_id} ha fatto {len(recent_txs)} transazioni in meno di {TIME_WINDOW_SECONDS} secondi")
                alerts_to_save.append({
                    "type": "VELOCITY_FRAUD", 
                    "user_id": user_id, 
                    "tx_count": len(recent_txs), 
                    "timestamp": datetime.now(timezone.utc).isoformat()
                })
            
            if is_amount_fraud:
                print(f"Transazione sospetta di {amount} EUR per l'utente {user_id}")
                alerts_to_save.append({
                    "type": "AMOUNT_FRAUD", 
                    "user_id": user_id, 
                    "amount": amount, 
                    "timestamp": datetime.now(timezone.utc).isoformat()
                })

            if not alerts_to_save:
                print(f"-> Transazione regolare per {user_id}.")
            else:
                for alert_data in alerts_to_save:
                    alerts_collection.insert_one(alert_data)
                    print(f"Alert [{alert_data['type']}] salvato nel database MongoDB")

    except KeyboardInterrupt:
        print("\n Interruzione ricevuta. Chiusura del consumer in corso...")
    finally:
        consumer.close()
        print("Consumer terminato correttamente.")

if __name__ == '__main__':
    main()