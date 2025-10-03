from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship

from .base import Base


class PlayerPet(Base):
    __tablename__ = 'player_pets'
    __table_args__ = {
        'extend_existing': True,
    }

    id = Column(Integer, primary_key=True, autoincrement=True)
    player_id = Column(Integer, ForeignKey('players.player_id'))
    item_id = Column(Integer, ForeignKey('items.item_id'))
    pet_name = Column(String(255), nullable=False)

    # Relationships
    player = relationship("Player", back_populates="pets")


