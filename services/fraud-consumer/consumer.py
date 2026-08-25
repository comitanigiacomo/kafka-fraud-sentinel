import os
import json
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv
from confluent_kafka import Consumer, Producer, KafkaError
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

# Leggo le credenziali del bot Telegram dal .env.
# Se non ci sono, le notifiche vengono semplicemente saltate.
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

def send_telegram_alert(message):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }
    try:
        requests.post(url, json=payload, timeout=5)
    except Exception as e:
        print(f"Avviso: notifica Telegram non inviata ({e})")

def get_ssl_base():
    # metto le opzioni SSL comuni in una funzione separata cosi' non le riscrivo due volte
    return {
        'bootstrap.servers': 'localhost:9092,localhost:9094,localhost:9095',
        'security.protocol': 'SSL',
        'ssl.ca.location': os.path.join(basedir, '../../security/ca.pem'),
        'ssl.certificate.location': os.path.join(basedir, '../../security/consumer.crt'),
        'ssl.key.location': os.path.join(basedir, '../../security/consumer.key.pem'),
        'ssl.endpoint.identification.algorithm': 'none',
    }

def get_consumer_config():
    cfg = get_ssl_base()
    cfg['group.id'] = 'fraud-detection-group'
    cfg['auto.offset.reset'] = 'earliest'
    return cfg

def get_producer_config():
    cfg = get_ssl_base()
    cfg['acks'] = 'all'
    return cfg

def delivery_report(err, msg):
    if err is not None:
        print(f"Errore invio alert sul topic fraud-alerts: {err}")

def main():
    consumer = Consumer(get_consumer_config())
    consumer.subscribe(['transactions'])

    # Il consumer e' anche un producer: quando trova una frode la pubblica
    # sul topic fraud-alerts, cosi' altri servizi possono reagire
    alert_producer = Producer(get_producer_config())

    # Dizionario per tracciare la cronologia degli utenti in memoria
    # Formato: { user_id: [timestamp_1, timestamp_2, ...] }
    user_activity_history = {}

    TIME_WINDOW_SECONDS = 10
    MAX_TRANSACTIONS_ALLOWED = 3

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                elif msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    continue
                else:
                    print(f"[-] Errore Consumer: {msg.error()}")
                    break

            try:
                tx = json.loads(msg.value().decode('utf-8'))
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue

            user_id = tx['user_id']
            amount = tx['amount']
            tx_time = datetime.fromisoformat(tx['timestamp']).timestamp()

            print(f"Analisi: {user_id} -> {amount} EUR")

            if user_id not in user_activity_history:
                user_activity_history[user_id] = []

            user_activity_history[user_id].append(tx_time)
            recent_txs = [t for t in user_activity_history[user_id] if (tx_time - t) <= TIME_WINDOW_SECONDS]
            user_activity_history[user_id] = recent_txs

            is_velocity_fraud = len(recent_txs) > MAX_TRANSACTIONS_ALLOWED
            is_amount_fraud = amount > 5000.0

            alerts_to_save = []

            if is_velocity_fraud:
                print(f"(Velocity Check): {user_id} ha fatto {len(recent_txs)} transazioni in {TIME_WINDOW_SECONDS} secondi")
                alerts_to_save.append({
                    "type": "VELOCITY_FRAUD",
                    "user_id": user_id,
                    "tx_count": len(recent_txs),
                    "timestamp": datetime.now(timezone.utc).isoformat()
                })

            if is_amount_fraud:
                print(f"Transazione sospetta di {amount} EUR per {user_id}")
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
                    # Salvo l'alert su MongoDB come prima
                    alerts_collection.insert_one(alert_data)
                    print(f"Alert [{alert_data['type']}] salvato nel database MongoDB")

                    # Rimuovo _id (aggiunto da PyMongo) prima di serializzare in JSON
                    alert_json = alert_data.copy()
                    if "_id" in alert_json:
                        del alert_json["_id"]

                    # Pubblico anche l'alert sul topic fraud-alerts.
                    alert_producer.produce(
                        topic='fraud-alerts',
                        key=user_id.encode('utf-8'),
                        value=json.dumps(alert_json).encode('utf-8'),
                        callback=delivery_report
                    )
                    alert_producer.poll(0)

                    # Mando notifica Telegram
                    if alert_data["type"] == "VELOCITY_FRAUD":
                        msg_text = (
                            f"🚨 *VELOCITY FRAUD*\n"
                            f"Utente: `{user_id}`\n"
                            f"Ha fatto {alert_data['tx_count']} transazioni in meno di {TIME_WINDOW_SECONDS} secondi"
                        )
                    else:
                        msg_text = (
                            f"💰 *AMOUNT FRAUD*\n"
                            f"Utente: `{user_id}`\n"
                            f"Importo sospetto: {alert_data['amount']:.2f} EUR"
                        )
                    send_telegram_alert(msg_text)

    except KeyboardInterrupt:
        print("\nInterruzione ricevuta. Chiusura in corso...")
    finally:
        alert_producer.flush(timeout=5)
        consumer.close()
        print("Consumer terminato correttamente.")

if __name__ == '__main__':
    main()
