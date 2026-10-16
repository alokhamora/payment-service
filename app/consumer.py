"""Consumer очереди payments.new: обработка платежа + webhook + retry/DLQ.

Запуск: faststream run app.consumer:app
"""

import asyncio
import logging
import random
import uuid

from faststream import FastStream
from faststream.rabbit import RabbitBroker, RabbitMessage
from sqlalchemy import update

from app.config import settings
from app.db import SessionFactory, engine
from app.messaging import (
    DLQ_NAME,
    NEW_QUEUE,
    declare_topology,
    retry_delay_seconds,
    retry_queue_name,
)
from app.models import Payment
from app.schemas import PaymentEvent, PaymentStatus, PaymentWebhook
from app.utils import utcnow
from app.webhook import send_webhook

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("payments.consumer")

broker = RabbitBroker(settings.rabbitmq_url)
app = FastStream(broker)


class PaymentNotFound(Exception):
    pass


@app.after_startup
async def _declare() -> None:
    await declare_topology(broker)


@app.after_shutdown
async def _dispose() -> None:
    await engine.dispose()


async def emulate_gateway() -> str:
    """Эмуляция внешнего платежного шлюза: 2–5 сек, 90% успех / 10% ошибка."""
    await asyncio.sleep(
        random.uniform(settings.processing_min_seconds, settings.processing_max_seconds)
    )
    ok = random.random() < settings.success_rate
    return (PaymentStatus.SUCCEEDED if ok else PaymentStatus.FAILED).value


async def process_payment(payment_id: uuid.UUID) -> None:
    """Идемпотентная обработка: каждый шаг пропускается, если уже выполнен.

    Сессии короткие — соединение с БД не удерживается на время эмуляции/HTTP.
    """
    async with SessionFactory() as session:
        payment = await session.get(Payment, payment_id)
        if payment is None:
            raise PaymentNotFound(str(payment_id))
        if payment.webhook_delivered_at is not None:
            logger.info("Payment %s already processed, skipping", payment_id)
            return
        needs_processing = payment.status == PaymentStatus.PENDING.value

    if needs_processing:
        new_status = await emulate_gateway()
        async with SessionFactory() as session, session.begin():
            # WHERE status='pending' — решение о статусе принимается ровно один раз
            await session.execute(
                update(Payment)
                .where(
                    Payment.id == payment_id,
                    Payment.status == PaymentStatus.PENDING.value,
                )
                .values(status=new_status, processed_at=utcnow())
            )
        logger.info("Payment %s processed: %s", payment_id, new_status)

    async with SessionFactory() as session:
        payment = await session.get(Payment, payment_id)
        assert payment is not None
        url = payment.webhook_url
        payload = PaymentWebhook.model_validate(payment).model_dump(mode="json")

    await send_webhook(url, payload)
    logger.info("Webhook for payment %s delivered to %s", payment_id, url)

    async with SessionFactory() as session, session.begin():
        await session.execute(
            update(Payment)
            .where(Payment.id == payment_id)
            .values(webhook_delivered_at=utcnow())
        )


async def _on_failure(event: PaymentEvent, attempt: int, exc: Exception) -> None:
    body = event.model_dump(mode="json")
    if attempt < settings.max_attempts:
        delay = retry_delay_seconds(attempt)
        logger.warning(
            "Attempt %d/%d for payment %s failed (%r); retry in %ds",
            attempt, settings.max_attempts, event.payment_id, exc, delay,
        )
        await broker.publish(
            body,
            queue=retry_queue_name(attempt),
            persist=True,
            headers={"x-attempt": attempt + 1},
        )
    else:
        logger.error(
            "Attempt %d/%d for payment %s failed (%r); moving to DLQ",
            attempt, settings.max_attempts, event.payment_id, exc,
        )
        await broker.publish(
            body,
            queue=DLQ_NAME,
            persist=True,
            headers={"x-attempt": attempt, "x-error": repr(exc)[:500]},
        )


@broker.subscriber(NEW_QUEUE, retry=False)
async def handle_new_payment(event: PaymentEvent, message: RabbitMessage) -> None:
    attempt = int((message.headers or {}).get("x-attempt", 1))
    try:
        await process_payment(event.payment_id)
    except Exception as exc:
        # Ошибка не пробрасывается: сообщение переотправляется в retry-очередь
        # (или в DLQ) и текущая копия подтверждается. Если сама публикация
        # упадёт — исключение приведёт к reject → dead-letter в payments.dlq.
        await _on_failure(event, attempt, exc)
