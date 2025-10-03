"""
Database Operations Module

This module provides comprehensive database operations for the DropTracker application.
It handles all database interactions including user management, drop tracking, group
operations, and data synchronization with external services.

Key Features:
- Drop insertion and validation
- User and group management
- Wise Old Man integration
- Redis caching operations
- Notification queue management
- Database session management

Classes:
    DatabaseOperations: Main class for all database operations

Author: DropTracker Team
"""

import json
from db import (
    GroupPatreon, GroupWomAssociation, NotifiedSubmission, Session, User, Group, Guild, Player, Drop, 
    UserConfiguration, session, XenforoSession, ItemList, GroupConfiguration, 
    GroupEmbed, Field as EmbField, NpcList, NotificationQueue, user_group_association, models
)
from dotenv import load_dotenv
from sqlalchemy.dialects import mysql
from sqlalchemy import func, text
from sqlalchemy.orm import joinedload
import interactions
from interactions import Embed
import os
import asyncio
from datetime import datetime, timedelta

# from db.xf.recent_submissions import create_xenforo_entry
# from utils.ranking.npc_ranker import check_npc_rank_change_from_drop
# from utils.ranking.rank_checker import check_rank_change_from_drop
from utils.embeds import get_global_drop_embed
from utils.download import download_player_image
from utils.format import normalize_player_display_equivalence
from utils.wiseoldman import fetch_group_members, check_user_by_id, check_user_by_username
from utils.redis import RedisClient, calculate_rank_amongst_groups, get_true_player_total
from utils.format import format_number, get_extension_from_content_type, parse_redis_data, parse_stored_sheet, replace_placeholders
#from utils.sheets.sheet_manager import SheetManager
#rom utils.semantic_check import check_drop as verify_item_real
#from db.item_validator import check_item_against_monster
import pymysql
from utils.redis import calculate_clan_overall_rank, calculate_global_overall_rank
from db.app_logger import AppLogger

load_dotenv()

insertion_lock = asyncio.Lock()

#sheets = SheetManager()

app_logger = AppLogger()

MAX_DROP_QUEUE_LENGTH = os.getenv("QUEUE_LENGTH")

redis_client = RedisClient()

global_footer = os.getenv('DISCORD_MESSAGE_FOOTER')

# Use a dictionary for efficient lookups
player_obj_cache = {}

class DatabaseOperations:
    """
    Main class for handling all database operations in the DropTracker system.
    
    This class provides methods for creating, updating, and managing database entities
    including users, players, groups, drops, and notifications. It also handles
    integration with external services like Wise Old Man and Redis caching.
    
    Attributes:
        drop_queue (list): Queue for batching drop insertions for performance
    """
    
    def __init__(self) -> None:
        """
        Initialize a new DatabaseOperations instance.
        
        Sets up the drop queue for batching database operations.
        """
        self.drop_queue = []
        pass

    async def create_drop_object(self, item_id, player_id, date_received, npc_id, value, quantity, image_url: str = "", authed: bool = False,
                                attachment_url: str = "", attachment_type: str = "", add_to_queue: bool = True, used_api: bool = False, unique_id: str = None, existing_session=None):
        """
        Create a drop object and optionally add it to the processing queue.
        
        This method creates a new Drop database entry with the provided information,
        handles image processing if attachments are provided, and can optionally
        add the drop to a queue for batch processing.
        
        Args:
            item_id (int): ID of the item that was dropped
            player_id (int): ID of the player who received the drop
            date_received (datetime|str): When the drop was received
            npc_id (int): ID of the NPC that dropped the item
            value (int): Grand Exchange value of the drop in GP
            quantity (int): Number of items dropped
            image_url (str, optional): URL to image of the drop. Defaults to "".
            authed (bool, optional): Whether the drop is authenticated. Defaults to False.
            attachment_url (str, optional): URL to attachment for processing. Defaults to "".
            attachment_type (str, optional): Type of attachment processing. Defaults to "".
            add_to_queue (bool, optional): Whether to add to processing queue. Defaults to True.
            used_api (bool, optional): Whether drop was submitted via API. Defaults to False.
            unique_id (str, optional): Unique identifier for deduplication. Defaults to None.
            existing_session (Session, optional): Database session to use. Defaults to None.
            
        Returns:
            Drop: The created Drop object
            
        Note:
            If attachment_url is provided, the method will attempt to download and process
            the image. The method handles various attachment types including direct downloads
            and Discord attachments.
        """
        db_session = session
        use_external_session = existing_session is not None
        if use_external_session:
            db_session = existing_session
        #print("Create_drop_object called")
        if isinstance(date_received, datetime):
            # Convert to string in the required format without timezone and microseconds
            date_received_str = date_received.strftime('%Y-%m-%d %H:%M:%S')
        else:
            date_received_str = date_received  # Assuming it's already a string in the correct format
        item = db_session.query(ItemList).filter(ItemList.item_id==item_id).first()
        item_name = item.item_name if item else "Unknown"
        npc = db_session.query(NpcList).filter(NpcList.npc_id==npc_id).first()
        npc_name = npc.npc_name if npc else "Unknown"
        if attachment_url and attachment_type:
            # Don't re-download if the image was already downloaded and processed
            if attachment_type == "downloaded":
                # Image was already downloaded, use the provided image_url directly
                #print(f"Using already downloaded image for drop {item_name} from {npc_name}")
                # The image_url should already be set from the original download
                pass
            else:
                # Download the image from the attachment URL
                player = db_session.query(Player).filter(Player.player_id == player_id).first()
                if not player:
                    #print(f"Player not found for drop {item_name} {npc_name} {player_id}")
                    return None
                try:
                    # Convert content type to proper file extension
                    file_extension = get_extension_from_content_type(attachment_type)
                    if int(value) * int(quantity) < get_xf_option("dt_min_value_for_image"):
                        pass
                    else:
                        download_path, image_url = await download_player_image("drop", item_name, player, attachment_url, file_extension, 0, item_name, npc_name)
                        if download_path is None or image_url is None:
                            #print(f"Failed to download image for drop {item_name} from {npc_name} - using empty image_url")
                            image_url = ""
                except Exception as e:
                    #print(f"Error downloading player image for {item_name} from {npc_name}: {type(e).__name__}: {e}")
                    import traceback
                    traceback.print_exc()
                    image_url = ""
        else:
            image_url = image_url or ""
        # Initialize image URL with the provided one

        # Create the drop object
        newdrop = Drop(item_id=item_id,
                    player_id=player_id,
                    date_added=date_received_str,
                    date_updated=date_received_str,
                    npc_id=npc_id,
                    value=value,
                    quantity=quantity,
                    authed=authed,
                    image_url=image_url,
                    used_api=used_api,
                    unique_id=unique_id)

        try:
            # Add the drop to the session and commit to generate the drop_id
            db_session.add(newdrop)
            db_session.commit()
        except Exception as e:
            db_session.rollback()
            print(f"Error committing new drop to the database: {e}")
            return None
        return newdrop

    async def create_user(self, auth_token, discord_id: str, username: str, ctx = None) -> User:
        """
        Create a new user in the database.
        
        Creates a new User entry with the provided Discord information and authentication token.
        The user can optionally be associated with an OSRS username.
        
        Args:
            auth_token (str): 16-character authentication token for API access
            discord_id (str): Discord user ID
            username (str): OSRS username (can be None)
            ctx (Context, optional): Discord command context for additional info. Defaults to None.
            
        Returns:
            User: The created User object
            
        Note:
            This method commits the database session automatically after creating the user.
        """
        new_user = User(discord_id=str(discord_id), auth_token=str(auth_token), username=str(username))
        try:
            session.add(new_user)
            session.commit()
            app_logger.log(log_type="new", data=f"{username} has been created with Discord ID {discord_id}", app_name="core", description="create_user")
            if ctx:
                return await ctx.send(f"Your Discord account has been successfully registered in the DropTracker database!\n" +
                                "You must now use `/claim-rsn` in order to claim ownership of your accounts.")
            default_config = session.query(UserConfiguration).filter(UserConfiguration.user_id == 1).all()
    ## grab the default configuration options from the database
            if new_user:
                user = new_user
            if not user:
                user = session.query(User).filter(User.discord_id == discord_id).first()

            new_config = []
            for option in default_config:
                option_value = option.config_value
                default_option = UserConfiguration(
                    user_id=user.user_id,
                    config_key=option.config_key,
                    config_value=option_value,
                    updated_at=datetime.now()
                )
                new_config.append(default_option)
            try:
                session.add_all(new_config)
                session.commit()
                return new_user
            except Exception as e:
                session.rollback()
            try:
                droptracker_guild: interactions.Guild = await ctx.bot.fetch_guild(guild_id=1172737525069135962)
                dt_member = droptracker_guild.get_member(member_id=discord_id)
                if dt_member:
                    registered_role = droptracker_guild.get_role(role_id=1210978844190711889)
                    await dt_member.add_role(role=registered_role)
            except Exception as e:
                print("Couldn't add the user to the registered role:", e)
            # xf_user = await xf_api.try_create_xf_user(discord_id=str(discord_id),
            #                                 username=username,
            #                                 auth_key=str(auth_token))
            # if xf_user:
            #     user.xf_user_id = xf_user['user_id']
        except Exception as e:
            session.rollback()
            app_logger.log(log_type="error", data=f"Couldn't create a new user with Discord ID {discord_id}: {e}", app_name="core", description="create_user")
            if ctx:
                return await ctx.send(f"`You don't have a valid account registered, " +
                            "and an error occurred trying to create one. \n" +
                            "Try again later, perhaps.`:" + e, ephemeral=True)
            if new_user:
                app_logger.log(log_type="new", data=f"{new_user.username} has been created with Discord ID {discord_id}", app_name="core", description="create_user")
                return new_user
            else:
                return None

    async def assign_rsn(user: User, player: Player):
        """
        Assign a player (RSN) to a user account.
        
        Creates the association between a Discord user and an OSRS player account.
        This allows the user to track drops and participate in group activities.
        
        Args:
            user (User): The User object to associate the player with
            player (Player): The Player object to associate with the user
            
        Note:
            This method commits the database session automatically after creating the association.
        """
        try:
            if not player.wom_id:
                return
            if player.user and player.user != user:
                """ 
                    Only allow the change if the player isn't already claimed.
                """
                app_logger.log(log_type="error", data=f"{user.username} tried to claim a rs account that was already associated with {player.user.username}'s account", app_name="core", description="assign_rsn")
                return False
            else:
                player.user = user
                session.commit()
                app_logger.log(log_type="access", data=f"{player.player_name} has been associated with {user.discord_id}", app_name="core", description="assign_rsn")
        except Exception as e:
            session.rollback()
            app_logger.log(log_type="error", data=f"Couldn't associate {player.player_name} with {user.discord_id}: {e}", app_name="core", description="assign_rsn")
            return False
        finally:
            return True
        
    async def get_group_embed(self, embed_type: str, group_id: int):
        """
        Retrieve a custom embed configuration for a specific group and embed type.
        
        Gets the configured embed template for a group, which can be customized
        for different types of notifications and displays. Falls back to default
        group (ID 1) configuration if no custom embed is found.
        
        Note: It is expected that verification against whether the group can use custom 
        embeds has already been completed before calling this method.

        Args:
            embed_type (str): Type of embed ("lb", "drop", "ca", "clog", "pb")
                - "lb": Lootboard/leaderboard embeds
                - "drop": Drop notification embeds  
                - "ca": Combat achievement embeds
                - "clog": Collection log embeds
                - "pb": Personal best embeds
            group_id (int): ID of the group to get embed configuration for
            
        Returns:
            Embed: Discord embed object with the group's custom configuration,
                   or default embed if no custom configuration exists
                   
        Note:
            This method handles placeholder replacement and field configuration
            based on the group's embed settings stored in the database.
        """
        try:
            stored_embed = session.query(GroupEmbed).filter(GroupEmbed.group_id == group_id, 
                                                            GroupEmbed.embed_type == embed_type).first()
            if not stored_embed:
                #print("No embed found for group", group_id, "and embed_type", embed_type)
                stored_embed = session.query(GroupEmbed).filter(GroupEmbed.group_id == 1,
                                                                GroupEmbed.embed_type == embed_type).first()
            if stored_embed:
                embed = Embed(title=stored_embed.title, 
                              description=stored_embed.description,
                              color=stored_embed.color)
                current_time = datetime.now()
                if stored_embed.timestamp:
                    embed.timestamp = current_time.timestamp()
                
                embed.set_thumbnail(url=stored_embed.thumbnail)
                embed.set_footer(global_footer)
                fields = session.query(EmbField).filter(EmbField.embed_id == stored_embed.embed_id).all()
                current_time = datetime.now()
                refresh_time = current_time + timedelta(minutes=10)
                refresh_unix = int(refresh_time.timestamp())
                if fields:
                    for field in fields:
                        field_name = str(field.field_name)
                        field_value = str(field.field_value)
                        field_name.replace("{next_refresh}", f"<t:{refresh_unix}:R>")
                        field_value.replace("{next_refresh}", f"<t:{refresh_unix}:R>")
                        embed.add_field(name=field_name,
                                        value=field.field_value,
                                        inline=field.inline)
                return embed
            else:
                print("No embed found")
                return None
        except Exception as e:
            app_logger.log(log_type="error", data=f"An error occurred trying to create a {embed_type} embed for group {group_id}: {e}", app_name="core", description="get_group_embed")
    
    async def create_notification(self, notification_type, player_id, data, group_id=None):
        """
        Create a notification queue entry.
        
        Adds a new entry to the notification queue for a specific player and group.
        The notification can be for a drop, achievement, or other event.
        
        Args:
            notification_type (str): Type of notification ("drop", "achievement", "other")
            player_id (int): ID of the player to notify
            data (dict): Additional data for the notification (e.g. drop details)
            group_id (int, optional): ID of the group to notify. Defaults to None.
        """
        notification = NotificationQueue(
            notification_type=notification_type,
            player_id=player_id,
            data=json.dumps(data),
            group_id=group_id,
            status='pending'
        )
        session.add(notification)
        session.commit()
        return notification.id
    
    async def create_player(self, player_name, account_hash) -> Player:
        """
        Create a player without Discord-specific functionality.
        
        Creates a new Player entry in the database with the provided OSRS username
        and account hash. Checks if the player already exists based on WOM ID or account hash.
        
        Args:
            player_name (str): OSRS username
            account_hash (str): Unique hash identifier for the OSRS account

        Returns:
            Player: The created Player object

        Note:
            This method creates a new player entry in the database and adds it to the session.
            It also creates a new player notification entry.    
        """
        account_hash = str(account_hash)
        
        try:
            # Check if player exists in WiseOldMan
            wom_player, player_name, wom_player_id, log_slots = await check_user_by_username(player_name)
            
            if not wom_player or not wom_player.latest_snapshot:
                return None
            
            player = session.query(Player).filter(Player.wom_id == wom_player_id).first()
            if not player:
                player = session.query(Player).filter(Player.account_hash == account_hash).first()
            
            if player is not None:
                if normalize_player_display_equivalence(player_name) != normalize_player_display_equivalence(player.player_name):
                    old_name = player.player_name
                    player.player_name = player_name
                    player.log_slots = log_slots
                    session.commit()
                    
                    # Create name change notification
                    notification_data = {
                        'player_name': player_name,
                        'player_id': player.player_id,
                        'old_name': old_name
                    }
                    await self.create_notification('name_change', player.player_id, notification_data)
            else:
                try:
                    overall = wom_player.latest_snapshot.data.skills.get('overall')
                    total_level = overall.level
                except Exception as e:
                    total_level = 0
                
                new_player = Player(
                    wom_id=wom_player_id, 
                    player_name=player_name, 
                    account_hash=account_hash, 
                    total_level=total_level,
                    log_slots=log_slots
                )
                session.add(new_player)
                session.commit()
                
                app_logger.log(log_type="access", data=f"{player_name} has been created with ID {new_player.player_id} (hash: {account_hash}) ", app_name="core", description="create_player")
                
                # Create new player notification
                notification_data = {
                    'player_name': player_name
                }
                await self.create_notification('new_player', new_player.player_id, notification_data)
                
                return new_player
        except Exception as e:
            app_logger.log(log_type="error", data=f"Error creating player: {e}", app_name="core", description="create_player")
            return None
        
        return player
    
    async def process_drop(self, drop_data, message_id=None, message_logger=None):
        """Process a drop submission and create notification entries if needed.
        
        Creates a new Drop entry in the database with the provided drop data.
        Processes the drop image if provided and creates notifications for the drop.
        
        Args:
            drop_data (dict): Drop data including item name, NPC name, value, quantity, etc.
            message_id (int, optional): Discord message ID. Defaults to None.
            message_logger (Logger, optional): Logger for message processing. Defaults to None.
        """
        # Extract drop data from the dictionary
        npc_name = drop_data.get('npc_name')
        item_name = drop_data.get('item_name')
        value = drop_data.get('value')
        item_id = drop_data.get('item_id')
        quantity = drop_data.get('quantity')
        auth_key = drop_data.get('auth_key')
        player_name = drop_data.get('player_name')
        account_hash = drop_data.get('account_hash')
        attachment_url = drop_data.get('attachment_url')
        attachment_type = drop_data.get('attachment_type')
        
        player_name = str(player_name).strip()
        account_hash = str(account_hash)
        
        # Get or create player
        player = session.query(Player).filter(Player.player_name.ilike(player_name)).first()
        if not player:
            player = await self.create_player(player_name, account_hash)
            if not player:
                return None
        
        player_id = player.player_id
        
        # Check NPC exists
        npc = session.query(NpcList).filter(NpcList.npc_name == npc_name).first()
        if not npc:
            # Create notification for new NPC
            notification_data = {
                'npc_name': npc_name,
                'player_name': player_name,
                'item_name': item_name,
                'value': value
            }
            
            await self.create_notification('new_npc', player_id, notification_data)
            return None
        
        npc_id = npc.npc_id
        
        # Check item exists
        item = session.query(ItemList).filter(ItemList.item_id == item_id).first()
        if not item:
            # Create notification for new item
            notification_data = {
                'item_name': item_name,
                'player_name': player_name,
                'item_id': item_id,
                'npc_name': npc_name,
                'value': value
            }
            
            await self.create_notification('new_item', player_id, notification_data)
            return None
        
        # Create the drop entry
        drop_value = int(value) * int(quantity)
        
        # Create the drop in the database
        drop = Drop(
            player_id=player_id,
            npc_id=npc_id,
            npc_name=npc_name,
            item_id=item_id,
            item_name=item_name,
            value=value,
            quantity=quantity,
            date_added=datetime.now()
        )
        
        session.add(drop)
        session.commit()
        
        # Process image if provided
        if attachment_url and attachment_type:
            try:
                # Set up the file extension and file name
                print(f"Debug - attachment_type: '{attachment_type}'")
                file_extension = get_extension_from_content_type(attachment_type)
                print(f"Debug - file_extension after conversion: '{file_extension}'")
                file_name = f"{item_id}_{npc_id}_{drop.drop_id}"
                
                if (value * quantity) > 50000:
                    dl_path, external_url = await download_player_image(
                        submission_type="drop",
                        file_name=str(file_name),
                        player=player,
                        attachment_url=str(attachment_url),
                        file_extension=str(file_extension),
                        entry_id=str(drop.drop_id),
                        entry_name=str(item_id),
                        npc_name=str(npc_id)
                    )
                
                    # Update the image URL in the drop entry
                    drop.image_url = external_url
                    session.commit()
            except Exception as e:
                app_logger.log(log_type="error", data=f"Couldn't download image: {e}", app_name="core", description="process_drop")
        
        # Get player groups
        player_groups = session.query(Group).join(
            user_group_association,
            (user_group_association.c.group_id == Group.group_id) &
            (user_group_association.c.player_id == player_id)
        ).all()
        
        # Create notifications for each group if the drop meets criteria
        for group in player_groups:
            group_id = group.group_id
            
            # Skip global group (ID 2)
            if group_id == 2:
                continue
                
            # Check if drop meets minimum value for notification
            min_value_config = session.query(GroupConfiguration).filter(
                GroupConfiguration.group_id == group_id,
                GroupConfiguration.config_key == 'min_value_to_notify'
            ).first()
            should_send_stacks = session.query(GroupConfiguration).filter(
                GroupConfiguration.group_id == group_id,
                GroupConfiguration.config_key == 'send_stacks_of_items'
            ).first()
            send_stacks = False
            if should_send_stacks:
                if should_send_stacks.config_value == '1' or should_send_stacks.config_value == 'true':
                    send_stacks = True
            
            min_value_to_notify = 1000000  # Default
            if min_value_config:
                min_value_to_notify = int(min_value_config.config_value)
            
            if int(value) >= min_value_to_notify or (send_stacks == True and int(drop_value) > min_value_to_notify):
                notification_data = {
                    'drop_id': drop.drop_id,
                    'item_name': item_name,
                    'npc_name': npc_name,
                    'value': value,
                    'quantity': quantity,
                    'total_value': drop_value,
                    'player_name': player_name,
                    'player_id': player_id,
                    'image_url': drop.image_url,
                    'attachment_type': attachment_type
                }
                
                await self.create_notification('drop', player_id, notification_data, group_id)
        
        return drop

def get_formatted_name(player_name:str, group_id: int, existing_session = None):
    """Get a formatted name for a player.
    
    Formats a player's name with a link to their profile and handles pinging in Discord.
    
    Args:
        player_name (str): OSRS username
        group_id (int): ID of the group to get the formatted name for
        existing_session (Session, optional): Database session to use. Defaults to None.
    Returns:
        str: The Discord-formatted name with ping-privacy considerations

    Note:
        This method handles pinging in Discord based on the user's settings.
    """
    # Determine which session to use
    use_existing_session = existing_session is not None
    if use_existing_session:
        db_session = existing_session
    else:
        db_session = session
    player = db_session.query(Player).filter(Player.player_name == player_name).first()
    formatted_name = f"[{player.player_name}](https://www.droptracker.io/players/{player.player_id}/view)"
    url_name = formatted_name
    if player.user:
        user: User = db_session.query(User).filter(User.user_id == player.user.user_id).first()
        if user:
            if group_id == 2 and user.global_ping:
                formatted_name = f"<@{user.discord_id}> ({url_name})"
            elif user.group_ping:
                formatted_name = f"<@{user.discord_id}> ({url_name})"
    return formatted_name
    

async def notify_group(bot: interactions.Client, type: str, group: Group, member: Player):
    configured_channel = session.query(GroupConfiguration).filter(GroupConfiguration.group_id == group.group_id, GroupConfiguration.config_key == "channel_id_to_send_logs").first()
    if configured_channel and configured_channel.config_value and configured_channel.config_value != "":
        channel_id = configured_channel.config_value
    else:
        return
    try:
        channel = await bot.fetch_channel(channel_id=channel_id)
    except Exception as e:
        print(f"Channel not found for ID: {channel_id}")
        return
    if type == "player_removed":
        if channel:
            if member.user:
                uid = f"<@{member.user.discord_id}>"
            else:
                uid = f"ID: `{member.player_id}`"
            embed = Embed(title=f"<:leave:1213802516882530375> Member Removed",
                          description=f"{member.player_name} ({uid}) has been removed from your group during to a WiseOldMan refresh.",
                          color=0x00ff00)
            query = """SELECT COUNT(*) FROM user_group_association WHERE group_id = :group_id"""
            total_players = session.execute(text(query), {"group_id": group.group_id}).fetchone()
            total_players = total_players[0] if total_players else 0
            embed.add_field(name="Total members:", value=f"{total_players}", inline=True)
            embed.set_footer(global_footer)
            await channel.send(embed=embed)
        else:
            print(f"Channel not found for ID: {channel_id}")
    elif type == "player_added":
        if channel:
            if member.user:
                uid = f"<@{member.user.discord_id}>"
            else:
                uid = f"ID: `{member.player_id}`"   
            embed = Embed(title=f"<:join:1213802515834204200> Member Added",
                          description=f"{member.player_name} ({uid}) has been added to your group during a WiseOldMan refresh.",
                          color=0x00ff00)
            query = """SELECT COUNT(*) FROM user_group_association WHERE group_id = :group_id"""
            total_members = session.execute(text(query), {"group_id": group.group_id}).fetchone()
            total_members = total_members[0] if total_members else 0
            embed.add_field(name="Total members:", value=f"{total_members}", inline=True)
            embed.set_footer(global_footer)
            await channel.send(embed=embed)
        else:
            print(f"Channel not found for ID: {channel_id}")

async def update_group_members(bot: interactions.Client, forced_id: int = None):
    app_logger.log(log_type="access", data="Updating group member association tables...", app_name="core", description="update_group_members")
    if forced_id:
        group_ids = [forced_id]
    else:
        # Use scalar_subquery to get just the values
        group_ids = session.scalars(session.query(Group.wom_id)).all()
    total_updated = 0
    for wom_id in group_ids:
        # wom_id should now be a simple integer
        #app_logger.log(log_type="access", data=f"Processing WOM ID: {wom_id}", app_name="core", description="update_group_members")
        try:
            wom_id = int(wom_id)
        except (ValueError, TypeError) as e:
            #app_logger.log(log_type="error", data=f"Error converting WOM ID to int: {e} - womid: {wom_id} (type: {type(wom_id)})", app_name="core", description="update_group_members")
            continue
        group: Group = session.query(Group).filter(Group.wom_id == wom_id).first()
        if group:
            group_wom_ids = await fetch_group_members(wom_id)
            #app_logger.log(log_type="ex_info", data=f"Group WOM IDs: {group_wom_ids}", app_name="core", description="update_group_members")
                
            # Only proceed with member updates if we successfully got the member list
            if group_wom_ids:
                ## We have a valid list of player wom_ids here now
                for player_wom_id in group_wom_ids:
                    try:
                        stored_association = session.query(GroupWomAssociation).filter(GroupWomAssociation.player_wom_id == player_wom_id,
                                                                                    GroupWomAssociation.group_dt_id == group.group_id).first()
                        if not stored_association:
                            new_association = GroupWomAssociation(player_wom_id=player_wom_id, group_dt_id=group.group_id)
                    except Exception as e:
                        print(f"Couldn't properly add a GroupWomAssociation for {player_wom_id} (player wom id) to {group.group_name}")
                # Get current group members from database
                group_members = session.query(Player).filter(Player.wom_id.in_(group_wom_ids)).all()
                # Remove members no longer in the group
                #app_logger.log(log_type="info", data=f"Found {len(group_members)} from our database in {group.group_name}", app_name="core", description="update_group_members")
                for member in group.players:
                    if member.wom_id and member.wom_id not in group_wom_ids:
                        
                        member = session.query(Player).filter(Player.player_id == member.player_id).first()
                        app_logger.log(log_type="access", data=f"{member.player_name} has been removed from {group.group_name}\nTheir DropTracker WOM ID is {member.wom_id} - group IDS: {group_wom_ids}", app_name="core", description="update_group_members")
                        member.remove_group(group)
                        try:
                            await notify_group(bot, "player_removed", group, member)
                        except Exception as e:
                            app_logger.log(log_type="error", data=f"Couldn't notify {group.group_name} that {member.player_name} has been removed: {e}", app_name="core", description="update_group_members")
                
                # Add new members to the group
                for member in group_members:
                    if member not in group.players:
                        if member.user:
                            member.user.add_group(group)
                        member.add_group(group)
                        member = session.query(Player).filter(Player.player_id == member.player_id).first()
                        try:
                            await notify_group(bot, "player_added", group, member)
                        except Exception as e:
                            pass
                group.date_updated = func.now()
                try:
                    session.commit()
                except Exception as e:
                    session.rollback()
            else:
                print(f"Failed to fetch member list for group {group.group_name} (WOM ID: {wom_id})")
        else:
            print("Group not found for wom_id", wom_id)

    ## Update the global group
    player_ids = session.query(Player.player_id).all()
    for player_id in player_ids:
        player = session.query(Player).filter(Player.player_id == player_id).first()
        if player:
            if 2 not in [group.group_id for group in player.groups]:
                player.add_group(session.query(Group).filter(Group.group_id == 2).first())
                session.commit()

async def associate_player_ids(player_wom_ids, before_date: datetime = None, session_to_use = None):
    # Query the database for all players' WOM IDs and Player IDs
    if session_to_use is not None:
        db_session = session_to_use
    else:
        db_session = session
    if before_date:
        all_players = db_session.query(Player.wom_id, Player.player_id).filter(Player.date_added < before_date).all()
    else:
        all_players = db_session.query(Player.wom_id, Player.player_id).all()
    if player_wom_ids is None:
        return []
    all_players = [player for player in all_players if player.player_id != None and player.wom_id != None]
    # Create a mapping of WOM ID to Player ID
    db_wom_to_ids = [{"wom": player.wom_id, "id": player.player_id} for player in all_players]
    
    # Filter out the Player IDs where the WOM ID matches any of the given `player_wom_ids`
    matched_ids = [player['id'] for player in db_wom_to_ids if player['wom'] in player_wom_ids]
    
    return matched_ids

event_session = None

def get_point_divisor():
    """
    Fetch the points divisor from XenForo options. Uses a safe, parameterized
    query and handles MariaDB/MySQL return types robustly.
    """
    return get_xf_option("dt_points_gp_per_point")

def get_xf_option(option_id: str):
    ## Assumes that the option will be stored under the standard table
    result = session.execute(
        text("SELECT option_value FROM xenforo.xf_option WHERE option_id = :option_id LIMIT 1"),
        {"option_id": option_id}
    ).scalar()
    if result is None:
        print(f"No option found for {option_id}, using default of 1000000")
        return 1000000
    try:
        if isinstance(result, (bytes, bytearray)):
            value_str = result.decode("utf-8", errors="ignore").strip()
        else:
            value_str = str(result).strip()
        lower_val = value_str.lower()
        if lower_val in ("true", "yes", "on"):
            return 1
        if lower_val in ("false", "no", "off"):
            return 0
        try:
            return int(value_str)
        except ValueError:
            return int(float(value_str))
    except Exception:
        return 1000000
    

async def update_group_members_silent(forced_id: int = None):
    """
    Duplicate of update_group_members function without Discord bot dependencies and notifications.
    Updates group member association tables silently without sending any Discord notifications.
    """
    app_logger.log(log_type="access", data="Updating group member association tables (silent mode)...", app_name="core", description="update_group_members_silent")
    if forced_id:
        group_ids = [forced_id]
    else:
        # Use scalar_subquery to get just the values
        group_ids = session.scalars(session.query(Group.wom_id)).all()
    total_updated = 0
    for wom_id in group_ids:
        # wom_id should now be a simple integer
        try:
            wom_id = int(wom_id)
        except (ValueError, TypeError) as e:
            continue
        group: Group = session.query(Group).filter(Group.wom_id == wom_id).first()
        if group:
            group_wom_ids = await fetch_group_members(wom_id)
            print("Got a total of ", len(group_wom_ids), "members for group ", group.group_name)
            # Only proceed with member updates if we successfully got the member list
            if group_wom_ids:
                ## We have a valid list of player wom_ids here now
                for player_wom_id in group_wom_ids:
                    try:
                        stored_association = session.query(GroupWomAssociation).filter(GroupWomAssociation.player_wom_id == player_wom_id,
                                                                                    GroupWomAssociation.group_dt_id == group.group_id).first()
                        if not stored_association:
                            new_association = GroupWomAssociation(player_wom_id=player_wom_id, group_dt_id=group.group_id)
                    except Exception as e:
                        print(f"Couldn't properly add a GroupWomAssociation for {player_wom_id} (player wom id) to {group.group_name}")
                # Get current group members from database
                group_members = session.query(Player).filter(Player.wom_id.in_(group_wom_ids)).all()
                # Remove members no longer in the group
                for member in group.players:
                    if member.wom_id and member.wom_id not in group_wom_ids:
                        member = session.query(Player).filter(Player.player_id == member.player_id).first()
                        app_logger.log(log_type="access", data=f"{member.player_name} has been removed from {group.group_name}\nTheir DropTracker WOM ID is {member.wom_id} - group IDS: {group_wom_ids}", app_name="core", description="update_group_members_silent")
                        member.remove_group(group)
                        # Note: Discord notification removed - silent operation
                        print(f"{member.player_name} has been removed from {group.group_name}\nTheir DropTracker WOM ID is {member.wom_id} - group IDS: {group_wom_ids}")
                # Add new members to the group
                for member in group_members:
                    if member not in group.players:
                        if member.user:
                            member.user.add_group(group)
                        member.add_group(group)
                        member = session.query(Player).filter(Player.player_id == member.player_id).first()
                        # Note: Discord notification removed - silent operation
                        print(f"{member.player_name} has been added to {group.group_name}\nTheir DropTracker WOM ID is {member.wom_id} - group IDS: {group_wom_ids}")
                group.date_updated = func.now()
                try:
                    session.commit()
                except Exception as e:
                    session.rollback()
            else:
                print(f"Failed to fetch member list for group {group.group_name} (WOM ID: {wom_id})")
        else:
            print("Group not found for wom_id", wom_id)

    ## Update the global group
    player_ids = session.query(Player.player_id).all()
    for player_id in player_ids:
        player = session.query(Player).filter(Player.player_id == player_id).first()
        if player:
            if 2 not in [group.group_id for group in player.groups]:
                player.add_group(session.query(Group).filter(Group.group_id == 2).first())
                session.commit()

async def get_ev_session():
    if event_session is None:
        event_session = Session()
    return event_session
