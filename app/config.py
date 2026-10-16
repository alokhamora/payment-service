from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://payments:payments@localhost:5432/payments"
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"

    # Аутентификация
    api_key: str = "change-me-secret-key"

    # Retry / DLQ: всего попыток обработки сообщения (включая первую)
    max_attempts: int = 3
    # Задержка перед n-й повторной попыткой: base * 2**(n-1) секунд (5s, 10s, ...)
    retry_base_delay_seconds: int = 5

    # Webhook
    webhook_timeout_seconds: float = 10.0

    # Outbox relay
    outbox_poll_interval_seconds: float = 0.5
    outbox_batch_size: int = 50

    # Эмуляция платежного шлюза
    processing_min_seconds: float = 2.0
    processing_max_seconds: float = 5.0
    success_rate: float = 0.9


settings = Settings()
