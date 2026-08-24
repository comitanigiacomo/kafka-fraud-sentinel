import os
import asyncio
from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient
from bson.json_util import dumps

basedir = os.path.abspath(os.path.dirname(__file__))
load_dotenv(os.path.join(basedir, '../../.env'))

mongo_client = MongoClient(
    host="localhost",
    port=27017,
    username=os.getenv("MONGO_USER"),
    password=os.getenv("MONGO_PASSWORD")
)
db = mongo_client["fraud_sentinel"]
alerts_collection = db["alerts"]

app = FastAPI(title="Fraud Sentinel API")

# Abilitaziione CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/api/alerts")

# Ritorna gli ultimi 50 allarmi dal database.
def get_historical_alerts():
     
    # Ordina gli allarmi per ID crescente e ritorna i primi 50.
    alerts = list(alerts_collection.find().sort("_id", -1).limit(50))
    return dumps(alerts)


@app.websocket("/ws/alerts")
async def websocket_alerts(websocket: WebSocket):
    await websocket.accept()
    
    last_known_count = alerts_collection.count_documents({})

    try:
        while True:
            # Conto i documenti attuali
            current_count = alerts_collection.count_documents({})
            
            # Se il numero è salito, il Consumer Kafka ha trovato una frode
            if current_count > last_known_count:
                
                new_alerts_count = current_count - last_known_count
                
                new_alerts = list(alerts_collection.find().sort("_id", -1).limit(new_alerts_count))
                
                for alert in new_alerts:
                    await websocket.send_text(dumps(alert))
                
                last_known_count = current_count
            
            await asyncio.sleep(2.0)

    except WebSocketDisconnect:
        print("Client disconnesso.")