"""
User Database Model

This module defines the User model which represents Discord users in the DropTracker system.
Users can be associated with multiple OSRS players and groups, and have various configuration
options for notifications and privacy settings.

Classes:
    User: Main user model representing Discord users

Author: joelhalen
"""

from sqlalchemy import Column, Integer, String, DateTime, Boolean, text
from sqlalchemy.dialects.mysql import TINYINT
from sqlalchemy import func
from sqlalchemy.orm import relationship

from .associations import user_group_association
from .base import session

from .base import Base


class User(Base):
    """
    Represents a Discord user in the DropTracker system.
    
    Users are the primary entity that connects Discord accounts to OSRS players
    and groups. Each user can have multiple players associated with their account
    and can be members of multiple groups.
    
    Attributes:
        user_id (int): Primary key, auto-incrementing user identifier
        discord_id (str): Discord user ID (up to 35 characters)
        date_added (datetime): Timestamp when user was first added
        auth_token (str): 16-character authentication token for API access
        date_updated (datetime): Timestamp of last update
        username (str): OSRS username (up to 20 characters)
        xf_user_id (int): XenForo user ID for forum integration
        public (bool): Whether user profile is publicly visible (default: True)
        global_ping (bool): Whether user will be pinged by the Discord bot when notifications are sent to the global discord server (default: False)
        group_ping (bool): Whether user will be pinged by the Discord bot when notifications are sent to their group discord server(s) (default: False)
        never_ping (bool): Whether user will never be pinged by the Discord bot when notifications are sent at all (default: False)
        hidden (bool): Whether user is hidden from public leaderboards/global channels (default: False)
    
    Relationships:
        players: List of Player objects associated with this user
        groups: List of Group objects this user is a member of
        configurations: List of UserConfiguration objects for this user
        group_patreon: List of GroupPatreon objects for this user
    """
    __tablename__ = 'users'
    __table_args__ = {
        'extend_existing': True,
    }

    user_id = Column(Integer, primary_key=True, autoincrement=True)
    discord_id = Column(String(35))
    date_added = Column(DateTime, default=func.now())
    auth_token = Column(String(16), nullable=False)
    date_updated = Column(DateTime, onupdate=func.now(), default=func.now())
    username = Column(String(20))
    xf_user_id = Column(Integer, nullable=True)
    public = Column(TINYINT(1), server_default=text('1'))
    global_ping = Column(Boolean, default=False)
    group_ping = Column(Boolean, default=False)
    never_ping = Column(Boolean, default=False)
    hidden = Column(Boolean, default=False)
    
    # Relationships
    players = relationship("Player", back_populates="user")
    groups = relationship("Group", secondary=user_group_association, back_populates="users", overlaps="groups")
    configurations = relationship("UserConfiguration", back_populates="user")
    group_patreon = relationship("GroupPatreon", back_populates="user")

    def add_group(self, group):
        """
        Add a group association to this user.
        
        Checks if the user is already associated with the group before adding
        to prevent duplicate associations.
        
        Args:
            group (Group): The Group object to associate with this user
            
        Note:
            This method commits the session automatically if a new association is created.
        """
        # Check if the association already exists by querying the user_group_association table
        existing_association = session.query(user_group_association).filter_by(
            user_id=self.user_id, group_id=group.group_id).first()

        if not existing_association:
            # Only add the group if no association exists
            self.groups.append(group)
            session.commit()


