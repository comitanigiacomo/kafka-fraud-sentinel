from confluent_kafka import Consumer

# attaccante senza TLS.
# Tenta di connettersi in chiaro su una porta che richiede SSL.
def get_insecure_config():
    return {
        'bootstrap.servers': 'localhost:9092,localhost:9094,localhost:9095',
        'security.protocol': 'PLAINTEXT',
        'group.id': 'attacker-group',
        'socket.timeout.ms': 5000
    }

def main():
    print("Tentativo di connessione in chiaro (PLAINTEXT) su porta SSL...")

    consumer = Consumer(get_insecure_config())
    consumer.subscribe(['test-transazioni'])

    try:
        msg = consumer.poll(5.0)

        if msg is None:
            print("Nessun messaggio ricevuto. Il broker ha rifiutato la connessione in chiaro.")
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