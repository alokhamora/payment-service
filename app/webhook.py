import httpx

from app.config import settings


async def send_webhook(url: str, payload: dict) -> None:
    """Отправляет webhook. Любой не-2xx ответ или сетевая ошибка → исключение."""
    async with httpx.AsyncClient(timeout=settings.webhook_timeout_seconds) as client:
        response = await client.post(
            url, json=payload, headers={"User-Agent": "payments-service/1.0"}
        )
        response.raise_for_status()
