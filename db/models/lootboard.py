from .base import Base
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy import func

class LootboardStyle(Base):
    __tablename__ = 'lootboards'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False)
    category = Column(String(255), nullable=False)
    description = Column(String(255), nullable=False)
    local_url = Column(String(255), nullable=False)
    date_added = Column(DateTime, default=func.now())
    date_updated = Column(DateTime, onupdate=func.now(), default=func.now())