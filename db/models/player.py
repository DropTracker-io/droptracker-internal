"""
Player Database Model

This module defines the Player model which represents OSRS player accounts in the DropTracker system.
Players are associated with Discord users and can be members of multiple groups. Each player
tracks their drops, personal bests, combat achievements, and other game-related data.

Classes:
    Player: Main player model representing OSRS accounts
    IgnoredPlayer: Model for players ignored in specific group leaderboards

Author: joelhalen
"""

from typing import List
from sqlalchemy import Column, Integer, String, Boolean, DateTime, ForeignKey
from sqlalchemy import func
from sqlalchemy.orm import relationship

from .associations import user_group_association
from .base import session

from .base import Base


class Player(Base):
    """
    Represents an OSRS player account in the DropTracker system.
    
    Players are the core entity for tracking OSRS game data including drops,
    personal bests, combat achievements, and collection log entries. Each player
    is associated with a Discord user and can be a member of multiple groups.
    
    Attributes:
        player_id (int): Primary key, auto-incrementing player identifier
        wom_id (int): Unique Wise Old Man player ID for integration
        account_hash (str): Unique hash identifier for the OSRS account (up to 100 chars)
        player_name (str): OSRS username (up to 20 characters)
        user_id (int): Foreign key to the associated User
        log_slots (int): Number of collection log slots unlocked
        total_level (int): Total skill level of the player
        date_added (datetime): Timestamp when player was first added
        date_updated (datetime): Timestamp of last update
        hidden (bool): Whether player is hidden from public displays (default: False)
    
    Relationships:
        user: Associated User object (Discord user)
        drops: List of Drop objects for this player
        pbs: List of PersonalBestEntry objects for this player
        cas: List of CombatAchievementEntry objects for this player
        clogs: List of CollectionLogEntry objects for this player
        pets: List of PlayerPet objects for this player
        groups: List of Group objects this player is a member of
        notifications: List of NotificationQueue objects for this player
        notified_submissions: List of NotifiedSubmission objects for this player
    """
    __tablename__ = 'players'
    __table_args__ = {
        'extend_existing': True,
    }

    player_id = Column(Integer, primary_key=True, autoincrement=True, index=True)
    wom_id = Column(Integer, unique=True)
    account_hash = Column(String(100), nullable=True, unique=True)
    player_name = Column(String(20), index=True)
    user_id = Column(Integer, ForeignKey('users.user_id'))
    log_slots = Column(Integer)
    total_level = Column(Integer)
    date_added = Column(DateTime, default=func.now())
    date_updated = Column(DateTime, onupdate=func.now(), default=func.now())
    hidden = Column(Boolean, default=False)
    
    # Relationships
    user = relationship("User", back_populates="players")
    drops = relationship("Drop", back_populates="player")
    pbs = relationship("PersonalBestEntry", back_populates="player")
    cas = relationship("CombatAchievementEntry", back_populates="player")
    clogs = relationship("CollectionLogEntry", back_populates="player")
    pets = relationship("PlayerPet", back_populates="player")
    groups = relationship("Group", secondary=user_group_association, back_populates="players")
    notifications = relationship("NotificationQueue", back_populates="player")
    # Backref for NotifiedSubmission.player
    notified_submissions = relationship("NotifiedSubmission", back_populates="player")

    def add_group(self, group):
        """
        Add this player to a group.
        
        Checks if the player is already associated with the group before adding
        to prevent duplicate associations. Also adds the group to the associated
        user if one exists.
        
        Args:
            group (Group): The Group object to associate with this player
            
        Note:
            This method commits the session automatically if a new association is created.
        """
        # Check if the association already exists by querying the user_group_association table
        existing_association = session.query(user_group_association).filter_by(
            player_id=self.player_id, group_id=group.group_id).first()
        if self.user:
            tuser = self.user
            tuser.add_group(group)
        if not existing_association:
            # Only add the group if no association exists
            self.groups.append(group)
            session.commit()

    def remove_group(self, group):
        """
        Remove this player from a group.
        
        Checks if the player is associated with the group before removing
        to prevent errors.
        
        Args:
            group (Group): The Group object to disassociate from this player
            
        Note:
            This method commits the session automatically if an association is removed.
        """
        # Check if the association already exists by querying the user_group_association table
        existing_association = session.query(user_group_association).filter_by(
            player_id=self.player_id, group_id=group.group_id).first()
        if existing_association:
            self.groups.remove(group)
            session.commit()

    def get_groups(self, session_to_use: None) -> List:
        """
        Get all groups this player is a member of.
        
        Args:
            session_to_use (Session, optional): Specific database session to use for the query
            
        Returns:
            List[Group]: List of Group objects this player is associated with
        """
        if session_to_use is not None:
            used_session = session_to_use
        else:
            used_session = session
        subquery = used_session.query(user_group_association.c.group_id).filter(user_group_association.c.player_id == self.player_id).all()
        from .group import Group
        groups = used_session.query(Group).filter(Group.group_id.in_(subquery)).all()
        return groups

    def get_current_total(self, npc_id: int = None, period: str = None):
        """
        Get the current total loot value for this player.
        
        Retrieves the player's total loot value from Redis cache for a specific
        time period and optionally for a specific NPC.
        
        Args:
            npc_id (int, optional): Specific NPC ID to get total for. If None, gets overall total.
            period (str, optional): Time period in format YYYYMM. If None, uses current month.
            
        Returns:
            int: Total loot value in GP for the specified criteria, or 0 if error occurs
            
        Note:
            This method uses Redis caching for performance. The partition format is
            year*100 + month (e.g., 202501 for January 2025).
        """
        from utils.redis import RedisClient
        redis_client = RedisClient()
        try:
            if not period:
                from datetime import datetime
                partition = datetime.now().year * 100 + datetime.now().month
            else:
                partition = period
            key = f"leaderboard:{partition}"
            if npc_id:
                key += f":{npc_id}"
            currentTotalBytes = redis_client.client.zscore(key, self.player_id)
            if currentTotalBytes is not None: 
                try:
                    if isinstance(currentTotalBytes, float) or isinstance(currentTotalBytes, int):
                        return int(currentTotalBytes)
                    else:
                        return int(float(currentTotalBytes.decode('utf-8')))
                except (ValueError, AttributeError):
                    return 0
            return 0
        except Exception as e:
            print(f"Error getting current total for player {self.player_id}: {e}")
            return 0

    def get_score_at_npc(self, npc_id: int, group_id: int = None, partition: int = None):
        """
        Get this player's score and rank for a specific NPC.
        
        Retrieves the player's loot value and leaderboard rank for a specific NPC,
        optionally within a specific group and time period.
        
        Args:
            npc_id (int): The NPC ID to get score for
            group_id (int, optional): Specific group to get rank within. If None, gets global rank.
            partition (int, optional): Time period in format YYYYMM. If None, uses current month.
            
        Returns:
            tuple: (rank, score) where rank is the leaderboard position (0-based) and 
                   score is the total loot value in GP. Both can be None if no data exists.
                   
        Note:
            This method uses Redis sorted sets for efficient ranking. The rank is 0-based,
            so rank 0 is the top player.
        """
        from datetime import datetime
        if partition is None:
            partition = datetime.now().year * 100 + datetime.now().month
        from utils.redis import RedisClient
        redis_client = RedisClient()
        base_key = 'leaderboard:'
        if (group_id != None):
            base_key += f'group:{group_id}:'
        base_key += f'npc:{npc_id}:'
        base_key += f'{partition}'
        if (base_key.endswith(':')):
            base_key = base_key[:-1]
        player_score = redis_client.client.zscore(base_key, self.player_id)
        player_rank = redis_client.client.zrank(base_key, self.player_id)
        return player_rank, player_score

    def __init__(self, wom_id, player_name, account_hash, user_id=None, user=None, log_slots=0, total_level=0, group=None, hidden=False):
        """
        Initialize a new Player instance.
        
        Args:
            wom_id (int): Wise Old Man player ID
            player_name (str): OSRS username
            account_hash (str): Unique hash identifier for the OSRS account
            user_id (int, optional): ID of the associated Discord user
            user (User, optional): Associated User object
            log_slots (int, optional): Number of collection log slots. Defaults to 0.
            total_level (int, optional): Total skill level. Defaults to 0.
            group (Group, optional): Initial group to associate with. Defaults to None.
            hidden (bool, optional): Whether player should be hidden. Defaults to False.
        """
        self.wom_id = wom_id
        self.player_name = player_name
        self.account_hash = account_hash
        self.user_id = user_id
        self.user = user
        self.log_slots = log_slots
        self.total_level = total_level
        self.hidden = hidden
        self.group = group


class IgnoredPlayer(Base):
    """
    Represents players that are ignored when generating lootboards for specific groups.
    
    This model allows groups to exclude certain players from their leaderboards
    without removing them from the group entirely. Useful for handling inactive
    players or special cases.
    
    Attributes:
        id (int): Primary key, auto-incrementing identifier
        player_id (int): Foreign key to the Player to be ignored
        group_id (int): Foreign key to the Group where the player should be ignored
        created_at (datetime): Timestamp when the ignore rule was created
        updated_at (datetime): Timestamp of last update
    """
    __tablename__ = 'ignored_players'
    id = Column(Integer, primary_key=True, autoincrement=True)
    player_id = Column(Integer, ForeignKey('players.player_id'), nullable=False)
    group_id = Column(Integer, ForeignKey('groups.group_id'), nullable=False)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, onupdate=func.now(), default=func.now())