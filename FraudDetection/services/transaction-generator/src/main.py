import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic_settings import BaseSettings

from src.infrastructure.kafka_producer import KafkaProducerClient
from src.simulator.engine import SimulationEngine

class Settings(BaseSettings):
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "bank.transactions.raw"
    events_per_second: int = 10

settings = Settings()

# Global dependencies
kafka_client = KafkaProducerClient(bootstrap_servers=settings.kafka_bootstrap_servers)
simulation_engine = SimulationEngine(kafka_client=kafka_client, topic=settings.kafka_topic)
simulation_task = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    kafka_client.start()
    yield
    # Shutdown
    if simulation_engine.running:
        simulation_engine.stop()
    if simulation_task:
        await simulation_task
    kafka_client.stop()


app = FastAPI(
    title="Transaction Generator Service",
    description="Simulates realistic banking transactions and publishes to Kafka.",
    version="1.0.0",
    lifespan=lifespan
)

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "simulation_running": simulation_engine.running,
        "kafka_connected": kafka_client.producer is not None
    }

@app.post("/start")
async def start_simulation():
    global simulation_task
    if simulation_engine.running:
        raise HTTPException(status_code=400, detail="Simulation is already running.")
    
    # Run the simulation engine in the background
    simulation_task = asyncio.create_task(simulation_engine.run(events_per_second=settings.events_per_second))
    
    return {"status": "started", "events_per_second": settings.events_per_second}

@app.post("/stop")
async def stop_simulation():
    if not simulation_engine.running:
        raise HTTPException(status_code=400, detail="Simulation is not running.")
    
    simulation_engine.stop()
    return {"status": "stopping"}
