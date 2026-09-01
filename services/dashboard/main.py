import os
import sys
import json
import subprocess
import asyncio
import threading
from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Response
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from confluent_kafka import Consumer as KafkaConsumer, KafkaError
from pymongo import MongoClient
from bson.json_util import dumps

basedir = os.path.abspath(os.path.dirname(__file__))
root_dir = os.path.abspath(os.path.join(basedir, '../../'))
load_dotenv(os.path.join(root_dir, '.env'))

mongo_client = MongoClient(
    host="localhost",
    port=27017,
    username=os.getenv("MONGO_USER"),
    password=os.getenv("MONGO_PASSWORD")
)
db = mongo_client["fraud_sentinel"]
alerts_collection = db["alerts"]

app = FastAPI(title="Fraud Sentinel API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory=os.path.join(basedir, "static")), name="static")

# tengo traccia di tutti i browser connessi via WebSocket
connected_clients = []

def get_kafka_dashboard_config():
    return {
        'bootstrap.servers': 'localhost:9092,localhost:9094,localhost:9095',
        'security.protocol': 'SSL',
        'ssl.ca.location': os.path.join(root_dir, 'security/ca.pem'),
        'ssl.certificate.location': os.path.join(root_dir, 'security/admin.crt'),
        'ssl.key.location': os.path.join(root_dir, 'security/admin.key.pem'),
        'ssl.endpoint.identification.algorithm': 'none',
        # Gruppo separato: la dashboard legge dal topic fraud-alerts
        # in modo indipendente dal fraud-detection-group principale
        'group.id': 'dashboard-group',
        'auto.offset.reset': 'latest'  # Mostra solo gli alert nuovi, non quelli vecchi
    }

def kafka_listener_thread(loop):
    # thread in background che legge dal topic fraud-alerts e manda
    # gli alert ai browser connessi tramite WebSocket
    consumer = KafkaConsumer(get_kafka_dashboard_config())
    consumer.subscribe(['fraud-alerts'])

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() in (KafkaError._PARTITION_EOF, KafkaError.UNKNOWN_TOPIC_OR_PART):
                    continue
                break

            try:
                alert = json.loads(msg.value().decode('utf-8'))
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue

            # Invio l'alert a tutti i client connessi via WebSocket
            for ws in list(connected_clients):
                asyncio.run_coroutine_threadsafe(ws.send_text(json.dumps(alert)), loop)

    finally:
        consumer.close()

@app.on_event("startup")
async def startup_event():
    # Avvio il thread Kafka in background quando parte il server
    loop = asyncio.get_event_loop()
    t = threading.Thread(target=kafka_listener_thread, args=(loop,), daemon=True)
    t.start()

@app.get("/")
def serve_dashboard():
    html_path = os.path.join(basedir, "index.html")
    with open(html_path, "r", encoding="utf-8") as f:
        html_content = f.read()
    return HTMLResponse(content=html_content)

@app.get("/api/alerts")
def get_historical_alerts():
    alerts = list(alerts_collection.find().sort("_id", -1).limit(50))
    return Response(content=dumps(alerts), media_type="application/json")

@app.post("/api/trigger/simulation")
def trigger_simulation():
    script_path = os.path.join(root_dir, "services/producer/producer.py")
    subprocess.Popen([sys.executable, script_path])
    return {"status": "started", "script": "producer.py"}

@app.post("/api/trigger/security-test")
def trigger_security_test():
    script_path = os.path.join(root_dir, "services/attacker/attacker_wrong_auth.py")
    subprocess.run([sys.executable, script_path], timeout=10)
    return {"status": "completed"}

@app.post("/api/trigger/stress-test")
def trigger_stress_test():
    # Lancia lo stress producer in background: manda una raffica di transazioni
    # per lo stesso utente, cosi' il consumer rileva il velocity fraud.
    script_path = os.path.join(root_dir, "services/stress-producer/stress_producer.py")
    subprocess.Popen([sys.executable, script_path])
    return {"status": "started", "script": "stress_producer.py"}

@app.websocket("/ws/alerts")
async def websocket_alerts(websocket: WebSocket):
    await websocket.accept()
    connected_clients.append(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        connected_clients.remove(websocket)
