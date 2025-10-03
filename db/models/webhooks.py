from sqlalchemy import Column, Integer, String, DateTime, Text
from sqlalchemy import func

from .base import Base


class Webhook(Base):
    __tablename__ = 'webhooks'
    __table_args__ = {
        'extend_existing': True,
    }

    id = Column(Integer, primary_key=True, autoincrement=True)
    webhook_id = Column(String(255), nullable=True)
    webhook_url = Column(String(255), unique=True)
    date_added = Column(DateTime, default=func.now())
    type = Column(String(255), nullable=True, default="core")
    date_updated = Column(DateTime, onupdate=func.now(), default=func.now())


class BackupWebhook(Base):
    __tablename__ = 'backup_webhooks'
    __table_args__ = {
        'extend_existing': True,
    }

    id = Column(Integer, primary_key=True, autoincrement=True)
    webhook_id = Column(String(255), nullable=True)
    webhook_url = Column(String(255), unique=True)
    type = Column(String(255), nullable=True, default="core")
    date_added = Column(DateTime, default=func.now())
    date_updated = Column(DateTime, onupdate=func.now(), default=func.now())


class WebhookPendingDeletion(Base):
    __tablename__ = 'webhook_pending_deletion'
    __table_args__ = {
        'extend_existing': True,
    }

    id = Column(Integer, primary_key=True, autoincrement=True)
    webhook_id = Column(String(255), nullable=True)
    webhook_url = Column(String(255), unique=True)
    channel_id = Column(String(255), nullable=True)
    date_added = Column(DateTime, default=func.now())
    date_updated = Column(DateTime, onupdate=func.now(), default=func.now())


class NewWebhook(Base):
    __tablename__ = 'new_webhooks'
    __table_args__ = {
        'extend_existing': True,
    }

    webhook_id = Column(Integer, primary_key=True)
    webhook_hash = Column(Text, unique=True)
    date_added = Column(DateTime, default=func.now())
    date_updated = Column(DateTime, onupdate=func.now(), default=func.now())


