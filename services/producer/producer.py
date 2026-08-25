import os
import time
import json
import random
from datetime import datetime, timezone
from dotenv import load_dotenv
from confluent_kafka import Producer

# Carico il file .env dalla root del progetto trovando il percorso assoluto
basedir = os.path.abspath(os.path.dirname(__file__))
load_dotenv(os.path.join(basedir, '../../.env'))

# Callback di Kafka: stampa se il messaggio è arrivato a destinazione o se c'è un errore
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
    users = ["user_101", "user_102", "user_103", "user_104", "user_105"]
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
    
    try:
        producer = Producer(get_kafka_config())
    except Exception as e:
        print(f"Errore critico di connessione al cluster Kafka: {e}")
        return

    topic_name = "transactions"

    try:
        counter = 1
        while True:
            tx = generate_mock_transaction()
            
            # Converto il dizionario in stringa JSON gestendo eventuali errori
            try:
                payload = json.dumps(tx)
            except (TypeError, ValueError) as serialization_err:
                print(f"Errore di serializzazione JSON: {serialization_err}")
                continue
            
            # Uso user_id così le transazioni dello stesso utente 
            # finiscono nella stessa partizione e mantengono l'ordine cronologico.
            user_key = tx["user_id"].encode('utf-8')
            
            producer.produce(
                topic=topic_name,
                key=user_key,
                value=payload.encode('utf-8'),
                callback=delivery_report
            )
            
            # Gestisco i callback in background
            producer.poll(0)
            
            print(f"-> Inviata transazione #{counter}: {tx['user_id']} ha speso {tx['amount']} {tx['currency']} presso {tx['merchant']}")
            counter += 1
            
            # Pausa tra una transazione e la successiva
            time.sleep(random.uniform(1, 2))
            
    except KeyboardInterrupt:
        print("\n Interruzione ricevuta. Svuotamento della coda in corso...")
        producer.flush(timeout=10)
        print("Producer terminato.")

if __name__ == '__main__':
    main()