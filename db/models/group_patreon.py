from sqlalchemy import Column, Integer, DateTime, ForeignKey
from sqlalchemy import func
from sqlalchemy.orm import relationship

from .base import Base


class GroupPatreon(Base):
    __tablename__ = 'group_patreon'
    __table_args__ = {
        'extend_existing': True,
    }

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey('users.user_id'), nullable=False)
    group_id = Column(Integer, ForeignKey('groups.group_id'), nullable=True)
    patreon_tier = Column(Integer, nullable=False)
    date_added = Column(DateTime, default=func.now())
    date_updated = Column(DateTime, onupdate=func.now(), default=func.now())
    
    # Relationships
    user = relationship("User", back_populates="group_patreon")
    group = relationship("Group", back_populates="group_patreon")


