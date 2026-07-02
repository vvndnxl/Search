import datetime
from typing import List

from pydantic import BaseModel, Field, ConfigDict
from sqlalchemy import Column, Integer, Text, DateTime, text
from sqlalchemy.dialects.postgresql import JSONB

from app.database import Base


class DocumentModel(Base):
    __tablename__ = "docs"

    id = Column(Integer, primary_key=True, index=True)
    rubrics = Column(JSONB, nullable=False)  # Массив рубрик в формате JSONB
    text = Column(Text, nullable=False)
    created_date = Column(DateTime, nullable=False, default=datetime.datetime.utcnow)


# =====================================================================
# 2. PYDANTIC SCHEMAS (DTO)
# =====================================================================
class DocumentResponse(BaseModel):
    id: int
    rubrics: List[str] = Field(..., description="Массив рубрик документа")
    text: str
    created_date: datetime.datetime

    model_config = ConfigDict(from_attributes=True)


class MessageResponse(BaseModel):
    detail: str