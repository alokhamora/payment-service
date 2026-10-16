import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, status
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_session
from app.messaging import NEW_QUEUE_NAME
from app.models import OutboxEvent, Payment
from app.schemas import PaymentCreate, PaymentCreated, PaymentOut, PaymentStatus
from app.security import verify_api_key
from app.utils import utcnow

router = APIRouter(
    prefix="/api/v1/payments",
    tags=["payments"],
    dependencies=[Depends(verify_api_key)],
)

SessionDep = Annotated[AsyncSession, Depends(get_session)]
IdempotencyKey = Annotated[
    str,
    Header(
        alias="Idempotency-Key",
        min_length=1,
        max_length=255,
        description="Уникальный ключ запроса для защиты от дублей",
    ),
]


def _same_request(payment: Payment, body: PaymentCreate) -> bool:
    return (
        payment.amount == body.amount
        and payment.currency == body.currency.value
        and payment.description == body.description
        and payment.meta == body.metadata
        and payment.webhook_url == str(body.webhook_url)
    )


async def _find_by_key(session: AsyncSession, key: str) -> Payment | None:
    return await session.scalar(select(Payment).where(Payment.idempotency_key == key))


def _replay(existing: Payment, body: PaymentCreate) -> PaymentCreated:
    if not _same_request(existing, body):
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            "Idempotency-Key was already used with a different request body",
        )
    return PaymentCreated.model_validate(existing)


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=PaymentCreated,
    summary="Создать платеж",
)
async def create_payment(
    body: PaymentCreate,
    idempotency_key: IdempotencyKey,
    session: SessionDep,
) -> PaymentCreated:
    if existing := await _find_by_key(session, idempotency_key):
        return _replay(existing, body)

    payment_id = uuid.uuid4()
    payment = Payment(
        id=payment_id,
        amount=body.amount,
        currency=body.currency.value,
        description=body.description,
        meta=body.metadata,
        status=PaymentStatus.PENDING.value,
        idempotency_key=idempotency_key,
        webhook_url=str(body.webhook_url),
        created_at=utcnow(),
    )
    # Outbox: платеж и событие сохраняются в ОДНОЙ транзакции
    session.add(payment)
    session.add(
        OutboxEvent(
            event_type="payment.created",
            queue=NEW_QUEUE_NAME,
            payload={"payment_id": str(payment_id)},
        )
    )
    try:
        await session.commit()
    except IntegrityError:
        # гонка: параллельный запрос с тем же ключом успел раньше
        await session.rollback()
        if existing := await _find_by_key(session, idempotency_key):
            return _replay(existing, body)
        raise
    return PaymentCreated.model_validate(payment)


@router.get(
    "/{payment_id}",
    response_model=PaymentOut,
    summary="Получить информацию о платеже",
)
async def get_payment(payment_id: uuid.UUID, session: SessionDep) -> PaymentOut:
    payment = await session.get(Payment, payment_id)
    if payment is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Payment not found")
    return PaymentOut.model_validate(payment)
