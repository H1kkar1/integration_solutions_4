from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import BaseModel


class RabbitSettings(BaseModel):
    host: str = "localhost"
    port: int = 5672
    username: str = "user"
    password: str = "123"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=False,
        env_nested_delimiter="__",
    )
    rmq: RabbitSettings


# Загружаем настройки явно
settings = Settings.model_validate({})