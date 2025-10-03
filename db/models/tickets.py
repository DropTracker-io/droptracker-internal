from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, func
from db.models.base import Base

class Ticket(Base):
    __tablename__ = 'tickets'
    ticket_id = Column(Integer, primary_key=True, autoincrement=True)
    channel_id = Column(String(255), nullable=False)
    type = Column(String(255), nullable=False)
    created_by = Column(Integer, ForeignKey('users.user_id'), nullable=False)
    claimed_by = Column(Integer, ForeignKey('users.user_id'), nullable=True)
    status = Column(String(255), nullable=False)
    date_added = Column(DateTime, default=func.now())
    last_reply_uid = Column(String(255), nullable=True)
    date_updated = Column(DateTime, onupdate=func.now(), default=func.now())