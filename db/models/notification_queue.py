from sqlalchemy import Column, Integer, String, Text, DateTime, ForeignKey, Index, UniqueConstraint
from datetime import datetime
from sqlalchemy.orm import relationship

from .base import Base


class NotificationQueue(Base):
    __tablename__ = 'notification_queue'
    __table_args__ = (
        UniqueConstraint('notification_type', 'player_id', 'group_id', 'data', name='uix_notification_unique'),
        Index('idx_notification_status_created', 'status', 'created_at'),
        Index('idx_notification_status', 'status'),
        {'extend_existing': True},
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    notification_type = Column(String(50), nullable=False)
    player_id = Column(Integer, ForeignKey('players.player_id'), nullable=False)
    data = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.now, nullable=False)
    processed_at = Column(DateTime, nullable=True)
    status = Column(String(20), default='pending', nullable=False)
    group_id = Column(Integer, ForeignKey('groups.group_id'), nullable=True)
    error_message = Column(Text, nullable=True)
    
    # Relationships
    player = relationship("Player", back_populates="notifications")
    group = relationship("Group", back_populates="notifications")


