from sqlalchemy import Column, Integer, String, Boolean

from .base import Base


class ItemList(Base):
    __tablename__ = 'items'
    __table_args__ = {
        'extend_existing': True,
    }

    item_id = Column(Integer, primary_key=True, nullable=False, index=True)
    item_name = Column(String(125), index=True)
    stackable = Column(Boolean, nullable=False, default=False)
    stacked = Column(Integer, nullable=False, default=0)
    noted = Column(Boolean, nullable=False)


