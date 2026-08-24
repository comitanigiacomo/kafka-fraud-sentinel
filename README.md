# Kafka Fraud Sentinel

This repository contains my university project for the Cloud Computing Technologies course at the University of Milan (UNIMI). It consists of a real-time fraud detection system designed to simulate a simple SOC (Security Operations Center). It monitors financial transactions to find anomalies, like amounts that are too high or too many transactions in a short time.

The project meets all the course requirements: it is a distributed system with high availability (HA) and fault tolerance, and it uses security features like encryption and authentication.

## Technologies used

The main part of the system is an Apache Kafka cluster running in KRaft mode with 3 nodes to ensure fault tolerance. The logic is written in Python using the `confluent-kafka` library. It works as a microservice architecture: one service produces the data and another reads it, showing exactly why a message queue is needed for this specific task. The detected alerts are then saved in a `MongoDB` database.

For the frontend, a web dashboard was built using `FastAPI` and `WebSockets` to send real-time updates to a simple `HTML`/`JS`/`CSS` page. To keep the system secure, `TLS encryption` and `SASL/PLAIN` authentication are configured for external clients.

## Repository structure

```text
├── security/              # TLS certificates, CA generation scripts, and broker credentials
├── services/
│   ├── attacker/          # Scripts simulating security violations (plain text, bad credentials)
│   ├── dashboard/         # FastAPI backend, WebSocket logic, and web interface
│   ├── demo/              # Scripts demonstrating consumer group state recovery
│   ├── fraud-consumer/    # Stream processing engine detecting fraud patterns
│   └── producer/          # Transaction generator injecting synthetic data
├── docker-compose.yml     # Multi-node Kafka cluster (KRaft) and MongoDB configuration
└── requirements.txt       # Python dependencies
```

## How to run the project

You only need Docker and Python installed on your system.

1. Start the infrastructure
Open a terminal in the project root and start the Kafka cluster and MongoDB using Docker Compose:

```bash
docker-compose up -d
```

2. start the conusmer
Open a second terminal, activate your Python virtual environment, and run the backend script that processes the data:

```bash
python services/consumer/consumer.py
```

3. start the web dashboard
Open a third terminal and run the FastAPI server:

```bash
uvicorn services.dashboard.main:app --reload --port 8000
```

You can now access the dashboard by opening `http://localhost:8000` in your browser.

# Testing and simulation

Once the dashboard is open, you can test the system using the UI controls. Please note that to see the dashboard update dynamically, the consumer script must be running in the background as explained in step 2.

Clicking the "Inietta Transazioni" button will trigger the producer script (`services/producer/producer.py`) in the background. It will start injecting fake financial transactions into the Kafka topics, and you will see the dashboard update as the consumer detects the anomalies.

To demonstrate robustness against security attacks, I included an audit feature. By clicking the "Test Audit/Auth" button, the backend will run a separate script (`scripts/attacker_wrong_auth.py`) that attempts to access the Kafka cluster using invalid SASL credentials. If you check the terminal, you will see the cluster actively rejecting the connection.

During the demonstration, it is also possible to show fault tolerance and high availability by simulating node failures (e.g., stopping a Kafka container while the data is flowing) without interrupting the service.

A complete description of the architectural choices, security configurations, and implementation details can be found in the final project report.