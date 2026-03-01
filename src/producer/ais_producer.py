import asyncio
import websockets
import json
import logging
import os
import time
from datetime import datetime, timezone
from kafka import KafkaProducer
from dotenv import load_dotenv

# --- CONFIGURACIÓN ---
load_dotenv()
AISSTREAM_API_KEY = os.getenv("AISSTREAM_API_KEY")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = 'vessel_positions'

# Bounding Box: Canarias Ampliado
BOUNDING_BOX = [[27.00, -18.50], [29.50, -13.00]]

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger(__name__)

def create_kafka_producer():
    """Crea la conexión a Kafka con TOLERANCIA A FALLOS (Espera a que Kafka arranque)"""
    retries = 30
    while retries > 0:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            logger.info("✅ Productor conectado a Kafka exitosamente.")
            return producer
        except Exception as e:
            logger.warning(f"⏳ Kafka no está listo. Reintentando en 5s... ({retries} intentos restantes)")
            retries -= 1
            time.sleep(5)
            
    logger.error("❌ Error Fatal: No se pudo conectar a Kafka tras 30 intentos.")
    return None

def parse_ais_message(message):
    """Normaliza mensajes Clase A y Clase B en un formato común."""
    msg_type = message.get("MessageType")
    meta = message.get("MetaData", {})
    
    lat = None
    lon = None
    speed = 0.0
    heading = 0.0
    mmsi = 0
    status = "Unknown"

    if msg_type == "PositionReport":
        payload = message["Message"]["PositionReport"]
        mmsi = payload["UserID"]
        lat = payload["Latitude"]
        lon = payload["Longitude"]
        speed = payload.get("Sog", 0.0)
        heading = payload.get("TrueHeading", 0.0)
        nav_status = payload.get("NavigationalStatus", 15)
        status = "Under way" if nav_status in [0, 8] else "Moored/Anchored"

    elif msg_type == "StandardClassBPositionReport":
        payload = message["Message"]["StandardClassBPositionReport"]
        mmsi = payload["UserID"]
        lat = payload["Latitude"]
        lon = payload["Longitude"]
        speed = payload.get("Sog", 0.0)
        heading = payload.get("TrueHeading", 511)
        if heading == 511: 
            heading = payload.get("Cog", 0.0)
        status = "Class B Active"
    else:
        return None 

    if lat is None or lon is None or lat > 90 or lat == 91 or lon > 180 or lon == 181:
        return None

    return {
        "mmsi": mmsi,
        "ship_name": meta.get("ShipName", f"Unknown-{mmsi}").strip(),
        "lat": lat,
        "lon": lon,
        "speed": speed,
        "heading": heading,
        "status": status,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

async def connect_ais_stream(producer):
    if not AISSTREAM_API_KEY:
        logger.error("❌ Falta API KEY en .env")
        return

    async with websockets.connect("wss://stream.aisstream.io/v0/stream") as websocket:
        subscribe_message = {
            "APIKey": AISSTREAM_API_KEY,
            "BoundingBoxes": [BOUNDING_BOX],
            "FilterMessageTypes": ["PositionReport", "StandardClassBPositionReport"] 
        }
        await websocket.send(json.dumps(subscribe_message))
        logger.info(f"📡 Escuchando tráfico marítimo (Clase A + B)...")

        async for message_json in websocket:
            try:
                message = json.loads(message_json)
                vessel_data = parse_ais_message(message)

                if vessel_data:
                    producer.send(KAFKA_TOPIC, vessel_data)
                    logger.info(f"📍 {vessel_data['ship_name']} ({vessel_data['status']}) -> Lat: {vessel_data['lat']:.4f}")

            except Exception as e:
                logger.error(f"⚠️ Error loop: {e}")

def main():
    producer = create_kafka_producer()
    if producer:
        try:
            asyncio.run(connect_ais_stream(producer))
        except KeyboardInterrupt:
            pass
        finally:
            producer.close()

if __name__ == "__main__":
    main()