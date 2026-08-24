import os
import time
import json
from dotenv import load_dotenv
from confluent_kafka import Consumer

basedir = os.path.abspath(os.path.dirname(__file__))
load_dotenv(os.path.join(basedir, '../../.env'))

# Connessione mTLS con il certificato admin (firmato dalla stessa CA dei broker)
# In questo modo si dimostra che qualsiasi client con un cert valido può connettersi.
def get_recovery_config():
    return {
        'bootstrap.servers': 'localhost:9092,localhost:9094,localhost:9095',
        'security.protocol': 'SSL',
        'ssl.ca.location': os.path.join(basedir, '../../security/ca.pem'),
        'ssl.certificate.location': os.path.join(basedir, '../../security/admin.crt'),
        'ssl.key.location': os.path.join(basedir, '../../security/admin.key.pem'),
        'ssl.endpoint.identification.algorithm': 'none',

        'group.id': 'demo-recovery-group', # Identificativo univoco del gruppo
        'auto.offset.reset': 'earliest',   # Se non c'è un segnalibro, parte dall'inizio
        'enable.auto.commit': True         # Kafka salva automaticamente l'offset in background
    }

def main():
    
    consumer = Consumer(get_recovery_config())
    consumer.subscribe(['test-transazioni'])

    try:
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
            if msg.error():
                continue
            
            try:
                raw_value = msg.value()
                if raw_value is None:
                    continue
                tx = json.loads(raw_value.decode('utf-8'))
            except (json.JSONDecodeError, UnicodeDecodeError):
                print(f"Ignorato messaggio corrotto all'offset {msg.offset()}")
                continue
            
            current_offset = msg.offset()
            
            print(f"Letta Transazione | Offset: {current_offset} | Utente: {tx['user_id']} | Importo: {tx['amount']} EUR")
            
            time.sleep(2)

    except KeyboardInterrupt:
        print("\nInterruzione ricevuta.")
    finally:
        consumer.close()
        print("Consumer disconnesso.")

if __name__ == '__main__':
    main()