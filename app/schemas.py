from datetime import datetime
from pydantic import BaseModel, ConfigDict


class DocumentResponse(BaseModel):
    """
    Схема валидации для ответа эндпоинтов поиска.
    Pydantic автоматически конвертирует datetime в ISO-строку.
    """
    id: int
    rubrics: list[str]
    text: str
    created_date: datetime

    # Конфигурация для поддержки ORM-моделей SQLAlchemy и словарей Redis
    model_config = ConfigDict(from_attributes=True)
