"""
Drop Database Model

This module defines the Drop model which represents individual item drops from NPCs
in the DropTracker system. Drops are the core data tracked by the application and
are used for leaderboards, notifications, and statistics.

Functions:
    get_current_partition: Calculate the current time partition for data organization

Classes:
    Drop: Main drop model representing individual item drops

Author: joelhalen
"""

from sqlalchemy import Column, Integer, String, Boolean, DateTime, ForeignKey
from sqlalchemy import func
from sqlalchemy.orm import relationship

from .base import Base


def get_current_partition() -> int:
    """
    Calculate the current partition identifier based on the current date.
    
    The partition is used to organize drops by time periods for efficient
    querying and leaderboard generation.
    
    Returns:
        int: Partition identifier in format YYYYMM (e.g., 202501 for January 2025)
    """
    from datetime import datetime
    now = datetime.now()
    return now.year * 100 + now.month


class Drop(Base):
    """
    Represents an individual item drop from an NPC in the DropTracker system.
    
    Drops are the core data entity tracked by the application. Each drop represents
    a single item obtained from killing an NPC, including metadata about the drop
    such as value, quantity, and authentication status.
    
    Attributes:
        drop_id (int): Primary key, auto-incrementing drop identifier
        item_id (int): Foreign key to the Item that was dropped
        player_id (int): Foreign key to the Player who received the drop
        date_added (datetime): Timestamp when the drop was recorded
        npc_id (int): Foreign key to the NPC that dropped the item
        date_updated (datetime): Timestamp of last update
        value (int): Grand Exchange value of the drop in GP
        quantity (int): Number of items dropped
        image_url (str): Optional URL to screenshot/image of the drop (up to 300 chars)
        authed (bool): Whether the drop has been authenticated/verified (default: False)
        used_api (bool): Whether the drop was submitted via API (default: False)
        partition (int): Time partition for efficient querying (default: current month)
        unique_id (str): Optional unique identifier for deduplication (up to 255 chars)
    
    Relationships:
        player: Associated Player object who received the drop
        notified_drops: List of NotifiedSubmission objects for this drop
    """
    __tablename__ = 'drops'
    __table_args__ = {
        'extend_existing': True,
    }

    drop_id = Column(Integer, primary_key=True, autoincrement=True)
    item_id = Column(Integer, ForeignKey('items.item_id'), index=True)
    player_id = Column(Integer, ForeignKey('players.player_id'), index=True, nullable=False)
    date_added = Column(DateTime, index=True, default=func.now())
    npc_id = Column(Integer, ForeignKey('npc_list.npc_id'), index=True)
    date_updated = Column(DateTime, onupdate=func.now(), default=func.now())
    value = Column(Integer)
    quantity = Column(Integer)
    image_url = Column(String(300), nullable=True)
    authed = Column(Boolean, default=False)
    used_api = Column(Boolean, default=False)
    partition = Column(Integer, default=get_current_partition, index=True)
    unique_id = Column(String(255), nullable=True)

    # Relationships
    player = relationship("Player", back_populates="drops")
    notified_drops = relationship("NotifiedSubmission", back_populates="drop")


