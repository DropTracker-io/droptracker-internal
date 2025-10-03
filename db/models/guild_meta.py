from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
from sqlalchemy import func
from sqlalchemy.orm import relationship

from .base import Base


class Guild(Base):
    __tablename__ = 'guilds'
    __table_args__ = {
        'extend_existing': True,
    }

    guild_id = Column(String(255), primary_key=True)
    group_id = Column(Integer, ForeignKey('groups.group_id'), nullable=True)
    date_added = Column(DateTime, default=func.now())
    date_updated = Column(DateTime, onupdate=func.now(), default=func.now())
    initialized = Column(Integer, default=0)
    
    # Relationships
    group = relationship("Group", back_populates="guild", single_parent=True, uselist=False)


class GroupWomAssociation(Base):
    __tablename__ = 'wom_group_member_ids'
    __table_args__ = {
        'extend_existing': True,
    }

    id = Column(Integer, primary_key=True, autoincrement=True)
    player_wom_id = Column(Integer, nullable=False)
    group_dt_id = Column(Integer, nullable=False)
    date_updated = Column(DateTime, onupdate=func.now(), default=func.now())


class GroupPersonalBestMessage(Base):
    __tablename__ = 'group_personal_best_message'
    __table_args__ = {
        'extend_existing': True,
    }

    id = Column(Integer, primary_key=True, autoincrement=True)
    group_id = Column(Integer, ForeignKey('groups.group_id'), nullable=False)
    message_id = Column(String(255), nullable=False)
    channel_id = Column(String(255), nullable=False)
    boss_name = Column(String(255), nullable=False)
    date_updated = Column(DateTime, onupdate=func.now(), default=func.now())


class LBUpdate(Base):
    __tablename__ = 'lb_updates'
    __table_args__ = {
        'extend_existing': True,
    }

    id = Column(Integer, primary_key=True)
    group_id = Column(Integer, nullable=False)
    date_updated = Column(DateTime, onupdate=func.now(), default=func.now())


