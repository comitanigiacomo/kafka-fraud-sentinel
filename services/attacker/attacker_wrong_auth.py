import os
from confluent_kafka import Consumer

basedir = os.path.abspath(os.path.dirname(__file__))

# Questo client supera il controllo TLS (ha il certificato), ma
# usa credenziali sbagliate per l'autenticazione SASL.
def get_wrong_auth_config():
    return {
        'bootstrap.servers': 'localhost:9092,localhost:9094,localhost:9095',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': 'hacker',          # Username inventato
        'sasl.password': 'password_errata', # Password inventata
        'ssl.ca.location': os.path.join(basedir, '../../security/ca.pem'),
        'group.id': 'attacker-group',
        'socket.timeout.ms': 5000
    }

def main():
    consumer = Consumer(get_wrong_auth_config())
    consumer.subscribe(['test-transazioni'])

    try:
        print("In attesa di messaggi")
        msg = consumer.poll(5.0)
        
        if msg is None:
            print("Nessun messaggio ricevuto. Il cluster ha rifiutato le credenziali.")
        elif msg.error():
            print(f"Il cluster ha bloccato l'attacco SASL: {msg.error()}")
        else:
            print("Attacco riuscito, cluster vulnerabile!")

    except Exception as e:
        print(f"Eccezione: {e}")
    finally:
        consumer.close()
        
if __name__ == '__main__':
    main()