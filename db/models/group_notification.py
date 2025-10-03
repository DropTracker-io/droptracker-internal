from sqlalchemy import Column, Integer, String, DateTime, Boolean, ForeignKey
from sqlalchemy import func
from sqlalchemy.orm import relationship

from .base import Base


class GroupNotification(Base):
    __tablename__ = 'group_notifications'
    __table_args__ = {
        'extend_existing': True,
    }

    id = Column(Integer, primary_key=True, autoincrement=True)
    group_id = Column(Integer, ForeignKey('groups.group_id'), nullable=False)
    title = Column(String(255), nullable=False)
    description = Column(String(1000), nullable=False)
    message = Column(String(255), nullable=False)
    jump_url = Column(String(255), nullable=True)
    type = Column(String(255), nullable=False)
    player_id = Column(Integer, ForeignKey('players.player_id'), nullable=True)
    item_id = Column(Integer, ForeignKey('items.item_id'), nullable=True)
    date_added = Column(DateTime, default=func.now())
    date_updated = Column(DateTime, onupdate=func.now(), default=func.now())
    status = Column(String(255), nullable=False)
    
    # Relationships
    # None back_populates were defined in legacy for this table.


