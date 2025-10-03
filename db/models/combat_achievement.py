from sqlalchemy import Column, Integer, String, Boolean, DateTime, ForeignKey
from sqlalchemy import func
from sqlalchemy.orm import relationship

from .base import Base


class CombatAchievementEntry(Base):
    __tablename__ = 'combat_achievement'
    __table_args__ = {
        'extend_existing': True,
    }

    id = Column(Integer, primary_key=True, autoincrement=True)
    player_id = Column(Integer, ForeignKey('players.player_id'))
    task_name = Column(String(255), nullable=False)
    image_url = Column(String(300), nullable=True)
    date_added = Column(DateTime, index=True, default=func.now())
    used_api = Column(Boolean, default=False)
    unique_id = Column(String(255), nullable=True)

    # Relationships
    player = relationship("Player", back_populates="cas")
    notified_ca = relationship("NotifiedSubmission", back_populates="ca")


