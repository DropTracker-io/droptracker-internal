from sqlalchemy import Column, Integer, String, Boolean, DateTime, ForeignKey
from sqlalchemy import func
from sqlalchemy.orm import relationship

from .base import Base


class CollectionLogEntry(Base):
    __tablename__ = 'collection'
    __table_args__ = {
        'extend_existing': True,
    }

    log_id = Column(Integer, primary_key=True, autoincrement=True)
    item_id = Column(Integer, index=True, nullable=False)
    npc_id = Column(Integer, ForeignKey('npc_list.npc_id'), nullable=False)
    player_id = Column(Integer, ForeignKey('players.player_id'), index=True, nullable=False)
    reported_slots = Column(Integer)
    image_url = Column(String(300), nullable=True)
    date_added = Column(DateTime, index=True, default=func.now())
    date_updated = Column(DateTime, onupdate=func.now(), default=func.now())
    used_api = Column(Boolean, default=False)
    unique_id = Column(String(255), nullable=True)

    # Relationships
    player = relationship("Player", back_populates="clogs")
    notified_clog = relationship("NotifiedSubmission", back_populates="clog")


