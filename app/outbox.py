"""Outbox relay: переносит события из таблицы outbox в RabbitMQ.

Гарантия — at-least-once: событие помечается опубликованным только после
подтверждения брокера (publisher confirms). Возможные дубликаты безопасны,
т.к. consumer идемпотентен.
"""

import asyncio
import contextlib
import logging

from faststream.rabbit import RabbitBroker
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.config import settings
from app.models import OutboxEvent
from app.utils import utcnow

logger = logging.getLogger("payments.outbox")


async def publish_pending(
    broker: RabbitBroker, factory: async_sessionmaker, batch_size: int
) -> int:
    published = 0
    async with factory() as session, session.begin():
        # SKIP LOCKED — можно безопасно запускать несколько реплик API
        events = (
            await session.scalars(
                select(OutboxEvent)
                .where(OutboxEvent.published_at.is_(None))
                .order_by(OutboxEvent.id)
                .limit(batch_size)
                .with_for_update(skip_locked=True)
            )
        ).all()
        for event in events:
            try:
                await broker.publish(
                    event.payload,
                    queue=event.queue,
                    persist=True,
                    message_id=str(event.id),
                    headers={"x-attempt": 1},
                )
            except Exception:
                logger.exception("Failed to publish outbox event %s", event.id)
                break
            event.published_at = utcnow()
            published += 1
    # коммит выше сохранит пометки только для реально опубликованных событий
    return published


async def run_outbox_relay(
    broker: RabbitBroker, factory: async_sessionmaker, stop: asyncio.Event
) -> None:
    logger.info("Outbox relay started")
    while not stop.is_set():
        try:
            count = await publish_pending(broker, factory, settings.outbox_batch_size)
        except Exception:
            logger.exception("Outbox relay iteration failed")
            count = 0
        if count:
            logger.info("Published %d outbox event(s)", count)
            continue
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(stop.wait(), settings.outbox_poll_interval_seconds)
    logger.info("Outbox relay stopped")
