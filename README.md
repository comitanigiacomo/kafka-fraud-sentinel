# Kafka Fraud Sentinel

This repository contains my university project for the Cloud Computing Technologies course at the University of Milan (UNIMI). It is a real-time fraud detection system that simulates a simple SOC (Security Operations Center). It monitors a stream of financial transactions looking for two types of anomalies: suspiciously high amounts and users making too many transactions in a short period of time (velocity fraud).

The project meets all the course requirements: the system is distributed with high availability and fault tolerance, and it enforces security with channel encryption and mutual authentication.

## Technologies used

The core infrastructure is an Apache Kafka cluster running in **KRaft mode** with 3 nodes. The application logic is written in Python using the `confluent-kafka` library. Detected fraud alerts are stored in a `MongoDB` database.

Security is handled via **mutual TLS (mTLS)**: each service (producer, consumer, admin/dashboard) has its own certificate signed by an internal CA. The broker enforces `ssl.client.auth=required`, so any client without a valid certificate is rejected at the TLS handshake level. Access Control Lists (ACLs) further restrict what each principal can read or write.

The frontend is a web dashboard built with `FastAPI`. It acts as a real Kafka consumer: a background thread subscribes to the `fraud-alerts` topic and pushes new alerts to the browser via **WebSockets** in real time. When a fraud alert is detected, the consumer also sends a **Telegram notification** to a configured bot.

## Architecture

The system uses two Kafka topics:

- `transactions` — the main stream. The producer writes all financial transactions here.
- `fraud-alerts` — the alert stream. The fraud-consumer reads from `transactions`, and when it detects something suspicious it writes an alert to this topic.

There are two distinct consumer groups:

- `fraud-detection-group` — the fraud-consumer, which does the actual detection.
- `dashboard-group` — the dashboard backend, which reads from `fraud-alerts` independently to display real-time updates.

## Repository structure

```text
├── demo/
│   └── demo-broker-failure.sh # script demonstrating cluster resilience during broker crashes
├── report/
│   ├── images/                # diagrams and screenshots used in the project report
│   ├── report.tex             # LaTeX source file of the project report
│   └── Kafka_Fraud_Sentinel.pdf # finalized project report
├── security/
│   ├── generate-certs.sh      # generates CA and all client/broker certificates
│   ├── setup-acls.sh          # configures Kafka ACLs (run once after startup)
│   └── broker-N-creds/        # per-broker TLS credentials
├── services/
│   ├── attacker/              # scripts simulating security violations (plaintext, wrong cert)
│   ├── dashboard/             # FastAPI backend + WebSocket + web interface
│   ├── demo/                  # script demonstrating consumer group offset recovery
│   ├── fraud-consumer/        # reads from 'transactions', detects fraud, writes to 'fraud-alerts'
│   ├── producer/              # generates and sends synthetic transactions at a normal pace
│   └── stress-producer/       # sends a burst of transactions to trigger velocity fraud
├── docker-compose.yml         # 3-node Kafka KRaft cluster + MongoDB
└── requirements.txt
```

## How to run the project

You need Docker and Python 3 installed.

**1. Start the infrastructure**

```bash
docker-compose up -d
```

**2. Configure ACL permissions** (run once, waits ~30 seconds for the cluster to be ready)

```bash
bash security/setup-acls.sh
```

**3. Install Python dependencies**

```bash
pip install -r requirements.txt
```

**4. (Optional) Configure Telegram notifications**

Edit the `.env` file and fill in your bot credentials:

```env
TELEGRAM_BOT_TOKEN=your_token_here
TELEGRAM_CHAT_ID=your_chat_id_here
```

If left empty, the consumer works normally without sending notifications.

**5. Start the fraud consumer**

```bash
python services/fraud-consumer/consumer.py
```

**6. Start the dashboard**

```bash
uvicorn services.dashboard.main:app --reload --port 8000
```

Open `http://localhost:8000` in your browser.

## Testing and demo

The dashboard has three buttons:

- **Inietta Transazioni** — starts the normal producer, which sends transactions at a rate of ~1 per second. Some will randomly have high amounts and trigger `AMOUNT_FRAUD` alerts.
- **Stress Test (Velocity)** — runs the stress producer, which sends 15 transactions for the same user in ~4 seconds, well above the threshold of 3 per 10 seconds. This triggers `VELOCITY_FRAUD` alerts.
- **Test Audit/Auth** — runs a script that tries to connect to the cluster without a valid client certificate. The connection is rejected at the TLS handshake level, demonstrating that mTLS works correctly.

**Fault tolerance demo:** to show that the cluster survives node failures, you can run the provided bash script or do it manually while the producer is running:

```bash
./demo/demo-broker-failure.sh
```
OR manually:
```bash
docker stop kafka-2
# producer keeps running without dropping messages
docker start kafka-2
# kafka-2 re-joins and catches up automatically
```
