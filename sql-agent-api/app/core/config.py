"""
Configuración central de la aplicación
"""
from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    """
    Configuración de la aplicación usando variables de entorno
    """
    
    # OpenAI
    OPENAI_API_KEY: str
    OPENAI_MODEL: str = "gpt-4o"
    OPENAI_EMBEDDING_MODEL: str = "text-embedding-3-small"
    
    # Database
    DATABASE_URL: str
    
    # Redis
    REDIS_URL: str = "redis://localhost:6379/0"
    
    # ChromaDB
    CHROMA_PERSIST_DIR: str = "./chroma_data"
    
    # Application
    APP_NAME: str = "SQL Agent API"
    APP_VERSION: str = "1.0.0"
    DEBUG: bool = True
    LOG_LEVEL: str = "INFO"
    
    # API
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8000
    API_RELOAD: bool = True
    
    # Exploration
    MAX_K_HOP: int = 2
    MAX_TABLES_EXPLORE: int = 10
    QUERY_TIMEOUT_SECONDS: int = 30
    
    # Cache
    CACHE_TTL_SECONDS: int = 86400  # 24 horas
    
    class Config:
        env_file = ".env"
        case_sensitive = True


# Instancia global de configuración
settings = Settings()