from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import BaseModel
from pydantic import PostgresDsn


class RabbitSettings(BaseModel):
    host: str = "localhost"
    port: int = 5672
    username: str
    password: str



class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file="app/.env",
        case_sensitive=False,
        env_nested_delimiter="__",
    )
    rmq: RabbitSettings


settings = Settings()
