from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
from sqlalchemy import func
from sqlalchemy.orm import relationship

from .base import Base


class UserConfiguration(Base):
    __tablename__ = 'user_configurations'
    __table_args__ = {
        'extend_existing': True,
    }

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey('users.user_id'), nullable=False)
    config_key = Column(String(60), nullable=False)
    config_value = Column(String(255), nullable=False)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    user = relationship("User", back_populates="configurations")


