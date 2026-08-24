import os
from confluent_kafka import Consumer

basedir = os.path.abspath(os.path.dirname(__file__))

# l'attaccante non ha un certificato client valido.
# Con ssl.client.auth=required il broker rifiuta il collegamento.
def get_no_cert_config():
    return {
        'bootstrap.servers': 'localhost:9092,localhost:9094,localhost:9095',
        'security.protocol': 'SSL',
        'ssl.ca.location': os.path.join(basedir, '../../security/ca.pem'),
        'ssl.endpoint.identification.algorithm': 'none',
        'group.id': 'attacker-group',
        'socket.timeout.ms': 5000
    }

def main():
    print("Tentativo di connessione senza certificato client...")

    consumer = Consumer(get_no_cert_config())
    consumer.subscribe(['test-transazioni'])

    try:
        msg = consumer.poll(5.0)

        if msg is None:
            print("Nessun messaggio ricevuto. Il broker ha rifiutato la connessione.")
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