from sqlalchemy import Column, Integer, String, Boolean, ForeignKey
from sqlalchemy.orm import relationship

from .base import Base


class GroupEmbed(Base):
    __tablename__ = 'group_embeds'
    __table_args__ = {
        'extend_existing': True,
    }

    embed_id = Column(Integer, primary_key=True, autoincrement=True)
    embed_type = Column(String(10))
    group_id = Column(Integer, ForeignKey('groups.group_id'), nullable=False, default=1)
    color = Column(String(7), nullable=True)
    title = Column(String(255), nullable=False)
    description = Column(String(1000), nullable=True)
    thumbnail = Column(String(200))
    timestamp = Column(Boolean, nullable=True, default=False)
    image = Column(String(200), nullable=True)

    fields = relationship("Field", back_populates="embed", cascade="all, delete-orphan")
    group = relationship("Group", back_populates="group_embeds")


class Field(Base):
    __tablename__ = 'group_embed_fields'
    __table_args__ = {
        'extend_existing': True,
    }

    field_id = Column(Integer, primary_key=True, autoincrement=True)
    embed_id = Column(Integer, ForeignKey('group_embeds.embed_id'), nullable=False)
    field_name = Column(String(256), nullable=False)
    field_value = Column(String(1024), nullable=False)
    inline = Column(Boolean, default=True)

    embed = relationship("GroupEmbed", back_populates="fields")


