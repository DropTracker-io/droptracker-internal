from sqlalchemy import Column, Integer, String, Boolean, DateTime, ForeignKey
from sqlalchemy import func
from sqlalchemy.orm import relationship

from .base import Base


class PersonalBestEntry(Base):
    __tablename__ = 'personal_best'
    __table_args__ = {
        'extend_existing': True,
    }

    id = Column(Integer, primary_key=True, autoincrement=True)
    player_id = Column(Integer, ForeignKey('players.player_id'))
    npc_id = Column(Integer, ForeignKey('npc_list.npc_id'))
    kill_time = Column(Integer, nullable=False)
    personal_best = Column(Integer, nullable=False)
    team_size = Column(String(15), nullable=False, default="Solo")
    new_pb = Column(Boolean, default=False)
    image_url = Column(String(300), nullable=True)
    date_added = Column(DateTime, nullable=True, default=func.now())
    used_api = Column(Boolean, default=False)
    unique_id = Column(String(255), nullable=True)

    # Relationships
    player = relationship("Player", back_populates="pbs")
    notified_pb = relationship("NotifiedSubmission", back_populates="pb")


