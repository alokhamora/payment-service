import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from faststream.rabbit import RabbitBroker

from app.api import router
from app.config import settings
from app.db import SessionFactory, engine
from app.messaging import declare_topology
from app.outbox import run_outbox_relay

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

broker = RabbitBroker(settings.rabbitmq_url)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    async with broker:  # connect / close
        await declare_topology(broker)
        stop = asyncio.Event()
        relay = asyncio.create_task(run_outbox_relay(broker, SessionFactory, stop))
        try:
            yield
        finally:
            stop.set()
            await relay
    await engine.dispose()


app = FastAPI(
    title="Payments Service",
    description="Асинхронная обработка платежей: API → outbox → RabbitMQ → consumer → webhook",
    version="1.0.0",
    lifespan=lifespan,
)
app.include_router(router)


@app.get("/health", include_in_schema=False)
async def health() -> dict[str, str]:
    return {"status": "ok"}
