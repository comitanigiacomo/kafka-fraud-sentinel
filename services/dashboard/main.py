import os
import sys
import subprocess
import asyncio
from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Response
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
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
    subprocess.Popen([sys.executable, script_path])
    return {"status": "started", "script": "attacker_wrong_auth.py"}

@app.websocket("/ws/alerts")
async def websocket_alerts(websocket: WebSocket):
    await websocket.accept()
    last_known_count = alerts_collection.count_documents({})
    try:
        while True:
            current_count = alerts_collection.count_documents({})
            if current_count > last_known_count:
                new_alerts_count = current_count - last_known_count
                new_alerts = list(alerts_collection.find().sort("_id", -1).limit(new_alerts_count))
                for alert in new_alerts:
                    await websocket.send_text(dumps(alert))
                last_known_count = current_count
            await asyncio.sleep(1.0)
    except WebSocketDisconnect:
        pass