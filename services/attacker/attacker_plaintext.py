import time
from confluent_kafka import Consumer, KafkaError

# non ha certificati SSL, non ha autenticazione SASL.
def get_insecure_config():
    return {
        'bootstrap.servers': 'localhost:9092',
        'security.protocol': 'PLAINTEXT', # Prova a connettersi in chiaro
        'group.id': 'attacker-group',
        'socket.timeout.ms': 5000
    }

def main():
    
    consumer = Consumer(get_insecure_config())
    consumer.subscribe(['test-transazioni'])

    try:
        msg = consumer.poll(5.0)
        
        if msg is None:
            print("Nessun messaggio ricevuto.")
        elif msg.error():
            print(f"Il cluster ha bloccato l'attacco: {msg.error()}")
        else:
            print("Attacco riuscito, cluster vulnerabile!")

    except Exception as e:
        print(f"Eccezione: {e}")
    finally:
        consumer.close()
if __name__ == '__main__':
    main()