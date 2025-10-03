from sqlalchemy import Column, Integer, String, DateTime, Boolean, ForeignKey, UniqueConstraint
from sqlalchemy import func
from sqlalchemy.orm import relationship

from .base import Base


class NotifiedSubmission(Base):
    __tablename__ = 'notified'
    __table_args__ = (
        UniqueConstraint('drop_id', 'clog_id', 'ca_id', 'pb_id', name='uix_notified_single_assoc'),
        {'extend_existing': True},
    )

    id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    channel_id = Column(String(35), nullable=False)
    message_id = Column(String(35))
    group_id = Column(Integer, ForeignKey('groups.group_id'), nullable=False)
    player_id = Column(Integer, ForeignKey('players.player_id'), nullable=True)
    status = Column(String(15))
    date_added = Column(DateTime, index=True, default=func.now())
    date_updated = Column(DateTime, onupdate=func.now(), default=func.now())
    edited_by = Column(Integer, ForeignKey('users.user_id'), nullable=True)
    drop_id = Column(Integer, ForeignKey('drops.drop_id'), nullable=True)
    clog_id = Column(Integer, ForeignKey('collection.log_id'), nullable=True)
    ca_id = Column(Integer, ForeignKey('combat_achievement.id'), nullable=True)
    pb_id = Column(Integer, ForeignKey('personal_best.id'), nullable=True)

    drop = relationship("Drop", back_populates="notified_drops")
    clog = relationship("CollectionLogEntry", back_populates="notified_clog")
    ca = relationship("CombatAchievementEntry", back_populates="notified_ca")
    pb = relationship("PersonalBestEntry", back_populates="notified_pb")
    player = relationship("Player", back_populates="notified_submissions")
    group = relationship("Group", back_populates="notified_submissions")


