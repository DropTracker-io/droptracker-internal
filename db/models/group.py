"""
Group Database Model

This module defines the Group model which represents OSRS clans/groups in the DropTracker system.
Groups can contain multiple players and users, and have various configuration options for
leaderboards, notifications, and integrations with external services like Wise Old Man.

Classes:
    Group: Main group model representing OSRS clans/groups

Author: joelhalen
"""

from sqlalchemy import Column, Integer, String, Boolean, DateTime
from sqlalchemy import func
from sqlalchemy.orm import relationship

from .associations import user_group_association
from .base import session

from .base import Base


class Group(Base):
    """
    Represents an OSRS clan/group in the DropTracker system.
    
    Groups are collections of players and users that share leaderboards,
    notifications, and other features. Each group can be associated with
    a Discord guild and integrated with external services.
    
    Attributes:
        group_id (int): Primary key, auto-incrementing group identifier
        group_name (str): Name of the group (up to 30 characters)
        description (str): Optional description of the group (up to 255 characters)
        date_added (datetime): Timestamp when group was created
        date_updated (datetime): Timestamp of last update
        wom_id (int): Wise Old Man group ID for integration
        guild_id (str): Discord guild ID associated with this group
        invite_url (str): Discord invite URL for the group
        icon_url (str): URL to the group's icon/logo
    
    Relationships:
        configurations: List of GroupConfiguration objects for this group
        players: Dynamic relationship to Player objects in this group
        users: List of User objects that are members of this group
        group_patreon: List of GroupPatreon objects for this group
        group_embeds: List of GroupEmbed objects for custom embed configurations
        guild: Associated Guild object for Discord integration
        notifications: List of NotificationQueue objects for this group
        notified_submissions: List of NotifiedSubmission objects for this group
    """
    __tablename__ = 'groups'
    __table_args__ = {
        'extend_existing': True,
    }

    group_id = Column(Integer, primary_key=True, autoincrement=True)
    group_name = Column(String(30), index=True)
    description = Column(String(255), nullable=True)
    date_added = Column(DateTime, default=func.now())
    date_updated = Column(DateTime, onupdate=func.now(), default=func.now())
    wom_id = Column(Integer, default=None)
    guild_id = Column(String(255), default=None, nullable=True)
    invite_url = Column(String(255), default=None, nullable=True)
    icon_url = Column(String(255), default=None, nullable=True)

    # Relationships
    configurations = relationship("GroupConfiguration", back_populates="group")
    players = relationship("Player", secondary=user_group_association, back_populates="groups", overlaps="groups", lazy='dynamic')
    users = relationship("User", secondary=user_group_association, back_populates="groups", overlaps="groups,players")
    group_patreon = relationship("GroupPatreon", back_populates="group")
    group_embeds = relationship("GroupEmbed", back_populates="group")
    guild = relationship("Guild", back_populates="group", uselist=False, cascade="all, delete-orphan")
    notifications = relationship("NotificationQueue", back_populates="group")
    # Backref for NotifiedSubmission.group
    notified_submissions = relationship("NotifiedSubmission", back_populates="group")

    def __init__(self, group_name, wom_id, guild_id, description: str= "An Old School RuneScape group."):
        """
        Initialize a new Group instance.
        
        Args:
            group_name (str): Name of the group
            wom_id (int): Wise Old Man group ID
            guild_id (str): Discord guild ID
            description (str, optional): Group description. Defaults to "An Old School RuneScape group."
        """
        self.group_name = group_name
        self.wom_id = wom_id
        self.guild_id = guild_id
        self.description = description

    def add_player(self, player):
        """
        Add a player to this group.
        
        Checks if the player is already associated with the group before adding
        to prevent duplicate associations.
        
        Args:
            player (Player): The Player object to add to this group
            
        Note:
            This method commits the session automatically if a new association is created.
        """
        # Check if the association already exists
        existing_association = self.players.filter(user_group_association.c.player_id == player.player_id).first()
        if not existing_association:
            # Only add the player if no association exists
            self.players.append(player)
            session.commit()

    def get_player_count(self, session_to_use=None):
        """
        Return the number of players in this group.

        With lazy='dynamic' the `players` relationship returns an
        `AppenderQuery`, so we must use `.count()` instead of `len()`.
        If an explicit session is supplied we count via the association
        table to stay within that session's context.
        
        Args:
            session_to_use (Session, optional): Specific database session to use for the query
            
        Returns:
            int: Number of players in this group
        """
        if session_to_use is not None:
            return (session_to_use
                    .query(user_group_association)
                    .filter(user_group_association.c.group_id == self.group_id,
                            user_group_association.c.player_id != None)
                    .count())

        return self.players.count()

    def get_players(self):
        """
        Return a concrete list of all players in the group.
        
        Since `self.players` is an AppenderQuery due to lazy='dynamic',
        we need to call `.all()` to get the actual list of players.
        
        Returns:
            List[Player]: List of all Player objects in this group
        """
        return self.players.all()

    def get_current_total(self):
        """
        Calculate the total loot value for all players in this group for the current month.
        
        Retrieves the total loot value from Redis cache for each player in the group
        for the current partition (year*100 + month).
        
        Returns:
            int: Total loot value in GP for all players in the group, or 0 if error occurs
            
        Note:
            This method uses Redis caching for performance. If Redis is unavailable
            or data is missing, it returns 0 for that player's contribution.
        """
        try:
            total_value = 0
            from utils.redis import RedisClient
            redis_client = RedisClient()
            for player in self.players:
                from datetime import datetime
                partition = datetime.now().year * 100 + datetime.now().month
                total_loot_key = f"player:{player.player_id}:{partition}:total_loot"
                total_loot = redis_client.client.get(total_loot_key)
                if total_loot:
                    total_loot = int(total_loot.decode('utf-8'))
                    total_value += total_loot
                else:
                    total_value += 0
            return total_value
        except Exception as e:
            print(f"Error getting current total for group {self.group_id}: {e}")
            return 0


