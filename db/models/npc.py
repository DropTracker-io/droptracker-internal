from sqlalchemy import Column, Integer, String

from .base import Base


class NpcList(Base):
    __tablename__ = 'npc_list'
    __table_args__ = {
        'extend_existing': True,
    }

    npc_id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    npc_name = Column(String(60), nullable=False)


