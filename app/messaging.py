"""Топология RabbitMQ — общая для API (outbox relay) и consumer-а.

payments.new ──(reject / исчерпаны попытки)──► payments.dlq
payments.new.retry.N ──(TTL = base * 2**(N-1))──► payments.new

Все dead-letter переходы идут через default exchange ("") по имени очереди.
"""

from faststream.rabbit import RabbitBroker, RabbitQueue

from app.config import settings

NEW_QUEUE_NAME = "payments.new"
DLQ_NAME = "payments.dlq"

NEW_QUEUE = RabbitQueue(
    NEW_QUEUE_NAME,
    durable=True,
    arguments={
        # страховка: если сообщение было reject-нуто (невалидное тело, сбой публикации
        # в retry-очередь и т.п.), оно попадёт в DLQ, а не потеряется
        "x-dead-letter-exchange": "",
        "x-dead-letter-routing-key": DLQ_NAME,
    },
)
DLQ = RabbitQueue(DLQ_NAME, durable=True)


def retry_queue_name(attempt: int) -> str:
    return f"{NEW_QUEUE_NAME}.retry.{attempt}"


def retry_delay_seconds(attempt: int) -> int:
    """Задержка перед повтором после `attempt`-й неудачной попытки."""
    return settings.retry_base_delay_seconds * 2 ** (attempt - 1)


def retry_queues() -> list[RabbitQueue]:
    # По отдельной очереди на каждую задержку: у очереди единый TTL, поэтому
    # сообщения с разным временем ожидания не блокируют друг друга.
    return [
        RabbitQueue(
            retry_queue_name(attempt),
            durable=True,
            arguments={
                "x-message-ttl": retry_delay_seconds(attempt) * 1000,
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": NEW_QUEUE_NAME,
            },
        )
        for attempt in range(1, settings.max_attempts)
    ]


async def declare_topology(broker: RabbitBroker) -> None:
    for queue in (NEW_QUEUE, DLQ, *retry_queues()):
        await broker.declare_queue(queue)
