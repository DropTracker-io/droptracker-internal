"""
Data Submissions Processing Module

This module handles the processing of various types of OSRS data submissions including
drops, personal bests, combat achievements, collection log entries, and pets. It provides
comprehensive validation, authentication, and notification systems for all submission types.

Key Features:
- Drop processing with value calculation and authentication
- Personal best tracking with deduplication logic
- Combat achievement verification and notifications
- Collection log entry processing
- Pet drop notifications
- Player creation and management
- Group notification systems
- Redis caching for performance optimization

Classes:
    SubmissionResponse: Response object for API submissions
    RawDropData: Container for raw drop submission data

Functions:
    drop_processor: Process drop submissions
    pb_processor: Process personal best submissions
    ca_processor: Process combat achievement submissions
    clog_processor: Process collection log submissions
    pet_processor: Process pet drop submissions
    
Author: joelhalen
"""

import asyncio
import hashlib
import time
from db import models, CombatAchievementEntry, Drop, FeatureActivation, NotifiedSubmission, PlayerPet, session, NpcList, Player, ItemList, PersonalBestEntry, CollectionLogEntry, User, Group, GroupConfiguration, UserConfiguration, NotificationQueue

# from db.update_player_total import update_player_in_redis
# from db.xf.recent_submissions import create_xenforo_entry
from services import redis_updates
# from services.lootboard_updater import instantly_update_board
from services.points import award_points_to_player
# from utils.embeds import update_boss_pb_embed
from utils.ge_value import get_true_item_value
# Removed circular import - these will be imported lazily inside functions
from utils import osrs_api
from utils.wiseoldman import check_user_by_id, check_user_by_username, check_group_by_id, fetch_group_members, get_collections_logged, get_player_boss_kills, get_player_metric
from utils.redis import RedisClient
from db.ops import DatabaseOperations, associate_player_ids, get_point_divisor
from utils.download import download_player_image, download_image
from sqlalchemy import func, text
from utils.format import convert_to_ms, format_number, get_command_id, get_extension_from_content_type, get_true_boss_name, replace_placeholders, convert_from_ms, normalize_player_display_equivalence
import interactions
from utils.logger import LoggerClient
from db.app_logger import AppLogger
from dotenv import load_dotenv
import os
import json
from datetime import datetime, timedelta
from sqlalchemy.engine import Row  # Add this import at the top
app_logger = AppLogger()

## Store a dict of the last time a group board update was forced to prevent immediate updates within 10 seconds of eachother
last_board_updates = {}

class SubmissionResponse:
    """
    Response object for API submission endpoints.
    
    Provides a standardized response format for all submission processors
    to communicate success/failure status and messages back to API clients.
    
    Attributes:
        success (bool): Whether the submission was processed successfully
        message (str): Descriptive message about the submission result
        notice (str, optional): Additional notice or warning information
    """
    
    def __init__(self, success, message, notice=None):
        """
        Initialize a new SubmissionResponse.
        
        Args:
            success (bool): Whether the submission was successful
            message (str): Message describing the result
            notice (str, optional): Additional notice information. Defaults to None.
        """
        self.success = success
        self.message = message
        self.notice = notice
        
"""

    Processes drops from the API endpoint and Discord Webhook endpoints

"""
load_dotenv()
debug_level = "false"  # Changed default to "true" for debugging

debug = debug_level != "false"

def debug_print(message, **kwargs):
    if debug:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}] DEBUG: {message}", **kwargs)

global_footer = os.getenv('DISCORD_MESSAGE_FOOTER')
redis_client = RedisClient()
db = DatabaseOperations()

last_channels_sent = []

            
async def run_restart():
    script = "./restart.sh"
    try:
        await asyncio.create_subprocess_shell(script)
    except Exception as e:
        debug_print(f"Error running restart script: {e}")

npc_list = {} # - stores a dict of the npc's and their corresponding IDs to prevent excessive querying
player_list = {} # - stores a dict of player name:ids, and their last refresh from the DB.
class RawDropData:
    """
    Container class for raw drop submission data.
    
    This class serves as a data structure to hold all the information
    related to a drop submission before it's processed and stored in the database.
    
    Note:
        This is primarily used as a type hint and data container for drop processing.
    """
    
    def __init__(self) -> None:
        """Initialize an empty RawDropData container."""
        pass  

def check_auth(player_name, account_hash, auth_key, external_session=None):
    """
    Returns true, true if there is a matching player+account_hash combo.
    Returns true, false if player exists but hash doesn't match.
    Returns false, false if player does not exist.
    """
    use_external_session = external_session is not None
    if use_external_session:
        db_session = external_session
    else:
        db_session = session
    try:
        player = db_session.query(Player).filter(Player.player_name.ilike(player_name)).first()
        
        if not player:
            return False, False
            
        if player.account_hash:
            if account_hash != player.account_hash:
                return True, False
            else:
                return True, True
        else:
            
            # Update the account hash if it's not set
            existing_player = db_session.query(Player).filter(Player.account_hash == account_hash).first()
            if existing_player:
                if normalize_player_display_equivalence(existing_player.player_name) != normalize_player_display_equivalence(player_name):
                    existing_player.player_name = player_name
                    app_logger.log(log_type="access", data=f"Player {player_name} already exists with account hash {account_hash}, updating player name to {player_name}", app_name="core", description="check_auth")
                    try:
                        db_session.commit()
                    except Exception as e:
                        debug_print("Error committing player name change:" + str(e))
                        db_session.rollback()
            player.account_hash = account_hash
            try:
                db_session.commit()
            except Exception as e:
                debug_print("Error committing player name change:" + str(e))
                db_session.rollback()  
            return True, True
    except Exception as e:
        debug_print("Error checking auth:" + str(e))
        return False, False


def select_session_and_flag(external_session):
    """Return (session, use_external_session_flag). When external is provided, caller manages commits."""
    use_external_session = external_session is not None
    if use_external_session:
        return external_session, True
    return session, False

async def ensure_item_for_drop(session, item_id, item_name):
    """Ensure an item exists by id or name. Mirrors drop processor behavior."""
    item = None
    if item_id is not None:
        item = session.query(ItemList).filter(ItemList.item_id == item_id).first()
    if not item and item_name is not None:
        try:
            async with osrs_api.create_client() as client:
                real_item = await client.semantic.check_item_exists(item_name)
            if real_item and item_id is not None:
                item = ItemList(item_name=item_name, item_id=item_id, noted=0, stackable=0, stacked=0)
                session.add(item)
                session.commit()
        except Exception:
            return None
    return item

async def ensure_player_and_auth(session, player_name, account_hash, auth_key):
    """Ensure player exists, cache id, and perform auth. Returns (player, authed, user_exists)."""
    player: Player = session.query(Player).filter(Player.account_hash == account_hash).first()
    if not player:
        player = await create_player(player_name, account_hash, existing_session=session)
        if not player:
            return None, False, False
    player_list[player_name] = player.player_id
    user_exists, authed = check_auth(player_name, account_hash, auth_key, session)
    return player, True, user_exists

unique_id_cache = {
    "clog": [],
    "drop": [],
    "pb": [],
    "ca": [],
    "pet": []
}

async def ensure_can_create(session, unique_id, submission_type) -> bool:
    """Ensure that a submission does not already exist in the database for this unique ID, with caching support for most-recent 1k of each entry.
    Returns True if it's safe to create (no duplicate exists), False if duplicate exists."""
    if unique_id in unique_id_cache[submission_type]:
        return False
    unique_id_cache[submission_type].append(unique_id)
    if len(unique_id_cache[submission_type]) > 1000:
        unique_id_cache[submission_type].pop(0)
    match submission_type:
        case "clog":
            existing_entry = session.query(CollectionLogEntry).filter(CollectionLogEntry.unique_id == unique_id,
                                                                    CollectionLogEntry.date_added > datetime.now() - timedelta(hours=1)).first()
            return False if existing_entry is not None else True
        case "drop":
            existing_entry = session.query(Drop).filter(Drop.unique_id == unique_id,
                                                        Drop.used_api == True,
                                                        Drop.date_added > datetime.now() - timedelta(hours=1)).first()
            return False if existing_entry is not None else True
        case "pb":
            existing_entry = session.query(PersonalBestEntry).filter(PersonalBestEntry.unique_id == unique_id,
                                                                    PersonalBestEntry.date_added > datetime.now() - timedelta(hours=1)).first()
            return False if existing_entry is not None else True
        case "ca":
            existing_entry = session.query(CombatAchievementEntry).filter(CombatAchievementEntry.unique_id == unique_id,
                                                                    CombatAchievementEntry.date_added > datetime.now() - timedelta(hours=1)).first()
            return False if existing_entry is not None else True
        case "pet":
            existing_entry = session.query(PlayerPet).filter(PlayerPet.unique_id == unique_id,
                                                            PlayerPet.date_added > datetime.now() - timedelta(hours=1)).first()
            return False if existing_entry is not None else True

async def ensure_npc_id_for_player(session, npc_name, player_id, player_name, use_external_session):
    """Resolve npc_id using cache, DB, or create via get_npc_id. If still unknown, queue 'new_npc' notification and return None."""
    if not npc_name:
        return None, None
    if npc_name in npc_list:
        return npc_list[npc_name], npc_name
    if ("doom of mokhaiotl" in npc_name.lower()) and ("(level" in npc_name.lower()):
        import re
        match = re.search(r"\(\s*Level\s*:??\s*(\d+)\s*\)", npc_name, flags=re.IGNORECASE)
        level_value = None
        if match:
            print("Got a match on doom level value:", match.group(1))
            level_value = int(match.group(1))
            try:
                ## Convert to int, as anything that isn't a single floor would send with a hyphen/etc and fail
                level_value = int(level_value)
            except Exception as e:
                ## If we don't find a proper floor value, return default doom id
                return 14704, npc_name
            npc_name = re.sub(r"\(\s*Level\s*:??\s*(\d+)\s*\)", r"(Level \1)", npc_name, flags=re.IGNORECASE)
            print("Parsed doom's name:", npc_name, "Level:", level_value)
            return (14707 + level_value), npc_name ## Doom's PB npc ids are stored by level; with each level 1 id incremented from the last
        return 14707, npc_name
    npc_row = session.query(NpcList.npc_id).filter(NpcList.npc_name == npc_name).first()
    if npc_row:
        npc_list[npc_name] = npc_row.npc_id
        return npc_row.npc_id, npc_name
    # Try to resolve from external source
    player_id = player_list.get(player_name)
    if player_id == 0:
        return None, npc_name
    try:
        async with osrs_api.create_client() as client:
            npc_id = await client.semantic.get_npc_id(npc_name)
        if npc_id:
            new_npc = NpcList(npc_id=npc_id, npc_name=npc_name)
            session.add(new_npc)
            session.commit()
            npc_list[npc_name] = npc_id
            return npc_id, npc_name
    except Exception:
        pass
    # Queue notification for unknown NPC and return None (preserves existing behavior)
    notification_data = {
        'npc_name': npc_name,
        'player_name': player_name,
        'player_id': player_id
    }
    await create_notification('new_npc', player_id, notification_data, existing_session=session if use_external_session else None)
    return None, npc_name

def resolve_attachment_from_drop_data(drop_data):
    """Return (attachment_url, attachment_type) based on existing drop_data logic."""
    downloaded = drop_data.get('downloaded', False)
    image_url = drop_data.get('image_url', None)
    if downloaded:
        return image_url, 'downloaded'
    if drop_data.get('attachment_type', None) is not None:
        return drop_data.get('attachment_url', None), drop_data.get('attachment_type', None)
    return image_url, None

def get_player_groups_with_global(session, player: Player):
    """Fetch groups via association table, ensure global group membership, and return list of Group objects."""
    global_group = session.query(Group).filter(Group.group_id == 2).first()
    player_gids = session.execute(text("SELECT group_id FROM user_group_association WHERE player_id = :player_id"), {"player_id": player.player_id}).all()
    player_groups = []
    if player_gids:
        for gid in player_gids:
            group = session.query(Group).filter(Group.group_id == gid[0]).first()
            if group:
                player_groups.append(group)
    if global_group and global_group not in player_groups:
        player.add_group(global_group)
        session.commit()
        player_groups.append(global_group)
    return player_groups

def is_truthy_config(value):
    if value is None:
        return False
    v = str(value).strip().lower()
    return v == 'true' or v == '1'

def get_group_drop_notify_settings(session, group_id):
    """Return (min_value_to_notify:int, send_stacks:bool)."""
    min_value_config = session.query(GroupConfiguration).filter(
        GroupConfiguration.group_id == group_id,
        GroupConfiguration.config_key == 'minimum_value_to_notify'
    ).first()
    min_value_to_notify = int(min_value_config.config_value) if min_value_config else 2500000
    should_send_stacks = session.query(GroupConfiguration).filter(
        GroupConfiguration.group_id == group_id,
        GroupConfiguration.config_key == 'send_stacks_of_items'
    ).first()
    send_stacks = is_truthy_config(should_send_stacks.config_value) if should_send_stacks else False
    return min_value_to_notify, send_stacks

def is_user_dm_enabled(session, user_id, key):
    cfg = session.query(UserConfiguration).filter(
        UserConfiguration.user_id == user_id,
        UserConfiguration.config_key == key
    ).first()
    return is_truthy_config(cfg.config_value) if cfg else False


async def ensure_item_by_name(session, item_name):
    """Ensure an item exists by name; resolves id via get_item_id when needed (used by clog/pb flows)."""
    if not item_name:
        return None
    item = session.query(ItemList).filter(ItemList.item_name == item_name).first()
    if item:
        return item
    try:
        async with osrs_api.create_client() as client:
            item_id = await client.semantic.get_item_id(item_name)
        if item_id:
            item = ItemList(item_name=item_name, item_id=item_id, noted=0, stackable=0, stacked=0)
            session.add(item)
            session.commit()
            return item
    except Exception:
        return None
    return None

async def ensure_player_by_name_then_auth(session, player_name, account_hash, auth_key):
    """Name-first player lookup (matching clog/pb/ca flows), create if missing, then auth. Returns (player, authed, user_exists)."""
    player = None
    if player_name:
        player: Player = session.query(Player).filter(Player.player_name.ilike(player_name)).first()
        if player and player.player_name != player_name:
            if player.account_hash == account_hash: ## Verify a matching account hash incase of some inconsistency 
            ## Always update the player's name if it doesn't match the incoming name
                player.player_name = player_name
                session.commit()    
    if not player:
        player = await create_player(player_name, account_hash, existing_session=session)
        if not player:
            return None, False, False
    player_list[player_name] = player.player_id
    user_exists, authed = check_auth(player_name, account_hash, auth_key, session)
    return player, authed, user_exists

async def clog_processor(clog_data, external_session=None):
    debug_print(f"=== CLOG PROCESSOR START ===")
    debug_print(f"Raw clog data: {clog_data}")
    debug_print(f"External session provided: {external_session is not None}")
    
    debug_test = False
    """Process a collection log submission and create notification entries if needed"""
    player_name = clog_data.get('player_name', clog_data.get('player', None))
    if player_name == "joelhalen":
        debug_test = True
    session, use_external_session = select_session_and_flag(external_session)
    debug_print(f"Using external session: {use_external_session}")
    if not player_name:
        debug_print("No player name found, aborting")
        return
    has_xf_entry = False

    account_hash = clog_data['acc_hash']
    item_name = clog_data.get('item_name', clog_data.get('item', None))
    if not item_name:
        debug_print("No item name found, aborting")
        return
    auth_key = clog_data.get('auth_key', '')
    attachment_url = clog_data.get('attachment_url', None)
    attachment_type = clog_data.get('attachment_type', None)
    reported_slots = clog_data.get('reported_slots', None)
    downloaded = clog_data.get('downloaded', False)
    image_url = clog_data.get('image_url', None)
    used_api = clog_data.get('used_api', False)
    killcount = clog_data.get('kc', None)
    unique_id = clog_data.get('guid', None)      
    item = await ensure_item_by_name(session, item_name)


    if not await ensure_can_create(session, unique_id, "clog"):
        print(f"Collection Log entry with Unique ID {unique_id} already exists in the database, aborting")
        return
    if not item:
        print(f"Item {item_name} not found in database, aborting")
        return
    item_id = item.item_id
    npc_name = clog_data.get('source', None)
    npc = npc_name
    print(f"NPC: {npc}")
    npc_id = None
    if player_name not in player_list and debug_test:
        await log_to_file(f"Debug test: {player_name} not in player_list")
    player, authed, user_exists = await ensure_player_by_name_then_auth(session, player_name, account_hash, auth_key)
    if not player:
        print(f"Player does not exist, and creating failed")
        return
    player_id = player_list[player_name]
    if debug_test:
      await log_to_file(f"Debug test: got player id after check {player_id}")
    npc_id, npc_name = await ensure_npc_id_for_player(session, npc_name, player_id, player_name, use_external_session)
    if npc_id is None:
        return
    # Validate player
    
    if debug_test:
      await log_to_file(f"Debug test: got npc id after check {npc_id}")
    # Get the player object for image download
    player = session.query(Player).filter(Player.player_id == player_id).first()
    if not player:
        print("Player not found in database, aborting")
        return
    if debug_test:
      await log_to_file(f"Debug test: got player object back after check {player}")
    if not user_exists or not authed:
        print("user failed auth check")
        return
        
    # Check if collection log entry already exists
    clog_entry = session.query(CollectionLogEntry).filter(
        CollectionLogEntry.player_id == player_id,
        CollectionLogEntry.item_id == item_id
    ).first()
    
    is_new_clog = False
    if npc_id is None:
        print(f"We did not find an npc for {npc_name}, aborting")
        return
    if not clog_entry:
        # Create new collection log entry
        if debug_test:
          await log_to_file(f"Debug test: This is new entry -- creating new clog entry")
        clog_entry = CollectionLogEntry(
            player_id=player_id,
            reported_slots=reported_slots,
            item_id=item_id,
            npc_id=npc_id,
            date_added=datetime.now(),
            image_url="",
            used_api=used_api,
            unique_id=unique_id
        )
        session.add(clog_entry)
        session.commit()  # Commit to get the log_id
        if debug_test:
          await log_to_file(f"Debug test: Committed session, log entry added: {clog_entry.log_id}")
        # Process image if available
        dl_path = ""
        if attachment_url and not downloaded:
            try:
                print(f"Debug - clog attachment_type: '{attachment_type}'")
                file_extension = get_extension_from_content_type(attachment_type)
                print(f"Debug - clog file_extension after conversion: '{file_extension}'")
                file_name = f"clog_{player_id}_{item_name.replace(' ', '_')}_{int(time.time())}"
                if debug_test:
                  await log_to_file(f"Debug test: Downloading image for {file_name}")
                dl_path, external_url = await download_player_image(
                    submission_type="clog",
                    file_name=file_name,
                    player=player,  # Now player is defined
                    attachment_url=attachment_url,
                    file_extension=file_extension,
                    entry_id=clog_entry.log_id,
                    entry_name=item_name
                )
                if debug_test:
                  await log_to_file(f"Debug test: Downloaded image for {file_name}")
                # Update the image URL - use external_url for serving, not the local path
                clog_entry.image_url = external_url if external_url else ""
            except Exception as e:
                app_logger.log(log_type="error", data=f"Couldn't download collection log image: {e}", app_name="core", description="clog_processor")
        elif downloaded:
            clog_entry.image_url = image_url
        
        is_new_clog = True
        print("Added clog to session")
    print("Committing session")
    session.commit()
    
    if debug_test:
      await log_to_file(f"Debug test: Added clog to session + committed")
    # Create notification if it's a new collection log entry
    if is_new_clog:
        print("New collection log -- Creating notification")
        if debug_test:
          await log_to_file(f"Debug test: New collection log -- Creating notification")
        # Get player groups
        award_points_to_player(player_id=player_id, amount=5, source=f'Collection Log slot: {item_name}', expires_in_days=60)
        
        
        player_groups = get_player_groups_with_global(session, player)
        for group in player_groups:
            print(f"CLOG: Checking group: {group}")
            group_id = group.group_id
            
            
            # Check if group has collection log notifications enabled
            clog_notify_config = session.query(GroupConfiguration).filter(
                GroupConfiguration.group_id == group_id,
                GroupConfiguration.config_key == 'notify_clogs'
            ).first()
            
            if debug_test:
              await log_to_file(f"Debug test: Checking group: {group} for clog notifications -- clog_notify_config: {clog_notify_config}")
            
            if clog_notify_config and (clog_notify_config.config_value.lower() == 'true' or int(clog_notify_config.config_value) == 1):
                if debug_test:
                  await log_to_file(f"Debug test: Group {group} has clog notifications enabled")
                notification_data = {
                    'player_name': player_name,
                    'player_id': player_id,
                    'item_name': item_name,
                    'npc_name': npc,
                    'image_url': clog_entry.image_url,
                    'kc_received': killcount,
                    'item_id': item_id
                }
                if debug_test:
                  await log_to_file(f"Debug test: Creating notification for group {group}")
                # if not has_xf_entry:
                #     await create_xenforo_entry(drop=None, clog=clog_entry, personal_best=None, combat_achievement=None)
                #     has_xf_entry = True
                if debug_test:
                  await log_to_file(f"Debug test: Created XenForo entry for clog")
                await create_notification('clog', player_id, notification_data, group_id, existing_session=session if use_external_session else None)
        if player:
            if player.user:
                user = session.query(User).filter(User.user_id == player.user_id).first()
                if user:
                    should_dm_cfg = session.query(UserConfiguration).filter(UserConfiguration.user_id == user.user_id,
                                                                            UserConfiguration.config_key == 'dm_clogs').first()
                    if should_dm_cfg:
                        should_dm = should_dm_cfg.config_value
                        should_dm = str(should_dm).lower()
                        if should_dm == "true" or should_dm == "1":
                            should_dm = True
                        else:
                            should_dm = False
                        if should_dm:
                            await create_notification('dm_clog', player_id, notification_data, group_id, existing_session=session if use_external_session else None)
                
    debug_print("Returning clog entry") 
    debug_print(f"=== CLOG PROCESSOR END ===")
    
    return clog_entry

async def ca_processor(ca_data, external_session=None):
    debug_print(f"=== CA PROCESSOR START ===")
    debug_print(f"Raw CA data: {ca_data}")
    debug_print(f"External session provided: {external_session is not None}")
    
    """Process a combat achievement submission and create notification entries if needed"""
    # global last_processor_run_at, last_processor_run
    has_xf_entry = False
    session, use_external_session = select_session_and_flag(external_session)
    debug_print(f"Using external session: {use_external_session}")
    player_name = ca_data['player_name']
    account_hash = ca_data['acc_hash']
    points_awarded = ca_data['points']
    points_total = ca_data['total_points']
    completed_tier = ca_data.get('completed', None)
    task_name = ca_data.get('task', None)
    tier = ca_data['tier']
    auth_key = ca_data.get('auth_key', '')
    attachment_url = ca_data.get('attachment_url', None)
    attachment_type = ca_data.get('attachment_type', None)
    downloaded = ca_data.get('downloaded', False)
    image_url = ca_data.get('image_url', None)
    used_api = ca_data.get('used_api', False)
    unique_id = ca_data.get('guid', None)
    if player_name == "Scributles":
        print(f"CA data for Scributles: {ca_data}")
    debug_print(f"Extracted CA data - Player: {player_name}, Task: {task_name}, Tier: {tier}")
    debug_print(f"Points awarded: {points_awarded}, Total points: {points_total}, Completed tier: {completed_tier}")
    debug_print(f"Account hash: {account_hash[:8]}... (truncated), Used API: {used_api}")
    # Validate player and auth
    player, authed, user_exists = await ensure_player_by_name_then_auth(session, player_name, account_hash, auth_key)
    if not player:
        debug_print("Player still not found in the database, aborting")
        return
    player_id = player.player_id
    if not user_exists or not authed:
        debug_print("User failed auth check")
        return


    if not await ensure_can_create(session, unique_id, "ca"):
        debug_print(f"Combat Achievement entry with Unique ID {unique_id} already exists in the database, aborting")
        return
    # Check if CA entry already exists
    ca_entry = session.query(CombatAchievementEntry).filter(
        CombatAchievementEntry.player_id == player_id,
        CombatAchievementEntry.task_name == task_name
    ).first()
    
    is_new_ca = False
    
    if not ca_entry:
        
        debug_print("CA entry not found in the database, creating new entry - Task tier: " + str(tier))
        dl_path = ""
        ca_entry = CombatAchievementEntry(
            player_id=player_id,
            task_name=task_name,
            date_added=datetime.now(),
            image_url=dl_path,
            used_api=used_api,
            unique_id=unique_id
        )
        session.add(ca_entry)
        is_new_ca = True
        # Process image if available
        if attachment_url and not downloaded:
            try:
                print(f"Debug - ca attachment_type: '{attachment_type}'")
                file_extension = get_extension_from_content_type(attachment_type)
                print(f"Debug - ca file_extension after conversion: '{file_extension}'")
                file_name = f"ca_{player_id}_{task_name.replace(' ', '_')}_{int(time.time())}"
                player = session.query(Player).filter(Player.player_id == player_id).first()
                if not player:
                    debug_print("Player not found in database, aborting")
                    return
                dl_path, external_url = await download_player_image(
                    submission_type="ca",
                    file_name=file_name,
                    player=player,
                    attachment_url=attachment_url,
                    file_extension=file_extension,
                    entry_id=ca_entry.id,
                    entry_name=task_name
                )
                
                # Use external_url for serving, not the local path
                if external_url:
                    ca_entry.image_url = external_url
            except Exception as e:
                app_logger.log(log_type="error", data=f"Couldn't download CA image: {e}", app_name="core", description="ca_processor")
        elif downloaded:
            ca_entry.image_url = image_url
    session.commit()
    debug_print("Committed a new CA entry")
    # Create notification if it's a new CA
    match str(tier).strip().lower():
        case 'easy':
            points = 1
        case 'medium':
            points = 2
        case 'hard':
            points = 3
        case 'elite':
            points = 4
        case 'master': 
            points = 5
        case 'grandmaster':
            points = 6
        case _:
            points = 1
    try:
        award_points_to_player(player_id=player_id, amount=points, source=f'Combat Achievement: {task_name}', expires_in_days=60)
    except Exception as e:
        debug_print(f"Couldn't award points to player: {e}")
        app_logger.log(log_type="error", data=f"Couldn't award points to player: {e}", app_name="core", description="ca_processor")
    if is_new_ca:
        debug_print("New CA entry, creating notification")
        # Get player groups
        player_groups = get_player_groups_with_global(session, player)
        
        for group in player_groups:
            debug_print("Checking group: " + str(group))
            group_id = group.group_id
            
            # Check if group has CA notifications enabled
            ca_notify_config = session.query(GroupConfiguration).filter(
                GroupConfiguration.group_id == group_id,
                GroupConfiguration.config_key == 'notify_cas'
            ).first()
            debug_print("CA notify config: " + str(ca_notify_config.config_value))
            if ca_notify_config and ca_notify_config.config_value.lower() == 'true' or ca_notify_config.config_value == '1':
                # Check if tier meets minimum notification tier
                min_tier = session.query(GroupConfiguration.config_value).filter(GroupConfiguration.config_key == 'min_ca_tier_to_notify',
                                                                            GroupConfiguration.group_id == group_id).first()
                tier_order = ['easy', 'medium', 'hard', 'elite', 'master', 'grandmaster']
                if min_tier != "disabled" or group_id == 2:
                    if (min_tier and min_tier[0].lower() in tier_order) or group_id == 2:
                        min_tier_value = min_tier[0].lower()
                        min_tier_index = tier_order.index(min_tier_value)
                        
                        # Check if the current task's tier meets the minimum requirement
                        task_tier_index = tier_order.index(tier.lower()) if tier.lower() in tier_order else -1
                        
                        if task_tier_index < min_tier_index:
                            # Task tier is below the minimum required tier, skip processing
                            debug_print(f"Skipping {task_name} ({tier}) as it's below minimum tier {min_tier_value} for group {group_id}")
                            continue
                        else:
                            debug_print("Tier meets minimum notification tier")
                            notification_data = {
                                'player_name': player_name,
                                'player_id': player_id,
                                'task_name': task_name,
                                'tier': tier,
                                'points_awarded': points_awarded,
                                'points_total': points_total,
                                'completed_tier': completed_tier,
                                'image_url': ca_entry.image_url
                            }
                            # if not has_xf_entry:
                            #     try:
                            #         await create_xenforo_entry(drop=None, clog=None, personal_best=None, combat_achievement=ca_entry)
                            #         has_xf_entry = True
                            #     except Exception as e:
                            #         debug_print(f"Couldn't add CA to XenForo: {e}")
                            #         app_logger.log(log_type="error", data=f"Couldn't add CA to XenForo: {e}", app_name="core", description="ca_processor")
                            if player:
                                if player.user:
                                    user = session.query(User).filter(User.user_id == player.user_id).first()
                                    if user:
                                        should_dm_cfg = session.query(UserConfiguration).filter(UserConfiguration.user_id == user.user_id,
                                                                                                UserConfiguration.config_key == 'dm_cas').first()
                                        if should_dm_cfg:
                                            should_dm = should_dm_cfg.config_value
                                            should_dm = str(should_dm).lower()
                                            if should_dm == "true" or should_dm == "1":
                                                should_dm = True
                                            else:
                                                should_dm = False
                                            if should_dm:
                                                await create_notification('dm_ca', player_id, notification_data, group_id, existing_session=session if use_external_session else None)
                            await create_notification('ca', player_id, notification_data, group_id, existing_session=session if use_external_session else None)
    
    debug_print(f"=== CA PROCESSOR END ===")
    return ca_entry

async def experience_processor(exp_data, external_session=None):
    debug_print("experience_processor")
    use_external_session = external_session is not None
    if use_external_session:
        db_session = external_session
    else:
        db_session = session
    player_name = exp_data['player_name']
    account_hash = exp_data['acc_hash']
    guid = exp_data['guid'] # unique submission hash
    last_trained = str(exp_data['last_skill_trained']).lower()
    levels_gained = exp_data['levels_earn']

async def quest_processor(quest_data, external_session=None):
    debug_print("quest_processor")
    use_external_session = external_session is not None
    if use_external_session:
        db_session = external_session
    else:
        db_session = session

async def pet_processor(pet_data, external_session=None):
    """Process a pet submission and create notification entries if needed"""
    debug_print(f"=== PET PROCESSOR START ===")
    debug_print(f"Raw pet data: {pet_data}")
    debug_print(f"External session provided: {external_session is not None}")
    
    session, use_external_session = select_session_and_flag(external_session)
    debug_print(f"Using external session: {use_external_session}")
    
    # Extract pet data
    player_name = pet_data.get('player_name', pet_data.get('player', None))
    if not player_name:
        debug_print("No player name found, aborting")
        return
    
    account_hash = pet_data.get('acc_hash', pet_data.get('account_hash', None))
    if not account_hash:
        debug_print("No account hash found, aborting")
        return
        
    pet_name = pet_data.get('pet_name', None)
    if not pet_name:
        debug_print("No pet name found, aborting")
        return
    
    auth_key = pet_data.get('auth_key', '')
    attachment_url = pet_data.get('attachment_url', None)
    attachment_type = pet_data.get('attachment_type', None)
    downloaded = pet_data.get('downloaded', False)
    image_url = pet_data.get('image_url', None)
    used_api = pet_data.get('used_api', False)
    source = pet_data.get('source', None)  # NPC/source that dropped the pet
    killcount = pet_data.get('killcount', None)
    milestone = pet_data.get('milestone', None)
    duplicate = pet_data.get('duplicate', False)
    previously_owned = pet_data.get('previously_owned', None)
    game_message = pet_data.get('game_message', None)
    unique_id = pet_data.get('guid', None)
    if not await ensure_can_create(session, unique_id, "pet"):
        print(f"Pet entry with Unique ID {unique_id} already exists in the database, aborting")
        return
    debug_print(f"Extracted pet data - Player: {player_name}, Pet: {pet_name}, Source: {source}")
    debug_print(f"Account hash: {account_hash[:8]}... (truncated), Duplicate: {duplicate}")
    debug_print(f"Attachment URL: {attachment_url}, Type: {attachment_type}, Downloaded: {downloaded}")
    
    has_xf_entry = False
    
    # Validate player and auth
    player, authed, user_exists = await ensure_player_by_name_then_auth(session, player_name, account_hash, auth_key)
    if not player:
        debug_print("Player not found in the database, aborting")
        return
    
    player_id = player.player_id
    if not user_exists or not authed:
        debug_print("User failed auth check")
        return
    
    # Try to find the pet item in the database
    pet_item = await ensure_item_by_name(session, pet_name)
    if not pet_item:
        debug_print(f"Pet item {pet_name} not found in database")
        # For pets, we might want to be more lenient and create a notification for unknown pets
        # rather than aborting completely
        pet_item_id = None
    else:
        pet_item_id = pet_item.item_id
        debug_print(f"Pet item validated - ID: {pet_item_id}, Name: {pet_name}")
    
    # Resolve NPC ID if source is provided
    npc_id = None
    npc_name = source
    if source:
        npc_id, npc_name = await ensure_npc_id_for_player(session, source, player_id, player_name, use_external_session)
        debug_print(f"NPC resolved - ID: {npc_id}, Name: {npc_name}")
    
    # Check if this pet already exists for this player (avoid duplicates)
    existing_pet = None
    new_pet = None
    if pet_item_id:
        existing_pet = session.query(PlayerPet).filter(
            PlayerPet.player_id == player_id,
            PlayerPet.item_id == pet_item_id
        ).first()
    
    is_new_pet = existing_pet is None
    
    # If it's a new pet and we have a valid item_id, store it
    if is_new_pet and pet_item_id:
        debug_print(f"Creating new pet entry for {player_name}: {pet_name}")
        try:
            new_pet = PlayerPet(
                player_id=player_id,
                item_id=pet_item_id,
                pet_name=pet_name
            )
            session.add(new_pet)
            session.commit()
            debug_print(f"Pet entry created successfully")
        except Exception as e:
            debug_print(f"Error creating pet entry: {e}")
            if not use_external_session:
                session.rollback()
            return
    elif existing_pet:
        debug_print(f"Pet {pet_name} already exists for player {player_name}")
    
    # Process image if available and it's a new pet
    dl_path = ""
    if is_new_pet and attachment_url and not downloaded:
        try:
            debug_print(f"Debug - pet attachment_type: '{attachment_type}'")
            file_extension = get_extension_from_content_type(attachment_type)
            debug_print(f"Debug - pet file_extension after conversion: '{file_extension}'")
            file_name = f"pet_{player_id}_{pet_name.replace(' ', '_')}_{int(time.time())}"
            
            dl_path, external_url = await download_player_image(
                submission_type="pet",
                file_name=file_name,
                player=player,
                attachment_url=attachment_url,
                file_extension=file_extension,
                entry_id=existing_pet.id if existing_pet else 0,
                entry_name=pet_name
            )
            
            # Use external_url for serving, not the local path
            if external_url:
                dl_path = external_url
        except Exception as e:
            app_logger.log(log_type="error", data=f"Couldn't download pet image: {e}", app_name="core", description="pet_processor")
    elif downloaded:
        dl_path = image_url
    if is_new_pet:
        award_points_to_player(player_id=player_id, amount=50, source=f'Pet: {pet_name}', expires_in_days=60)
    # Create notifications for new pets or duplicates (depending on configuration)
    should_notify = is_new_pet or (duplicate and not is_new_pet)
    
    if should_notify:
        debug_print(f"Creating notifications for pet submission")
        
        # Get player groups
        player_groups = get_player_groups_with_global(session, player)
        
        for group in player_groups:
            debug_print(f"Checking group: {group.group_name}")
            group_id = group.group_id
            
            # Check if group has pet notifications enabled
            pet_notify_config = session.query(GroupConfiguration).filter(
                GroupConfiguration.group_id == group_id,
                GroupConfiguration.config_key == 'notify_pets'
            ).first()
            
            debug_print(f"Pet notify config for group {group_id}: {pet_notify_config.config_value if pet_notify_config else 'None'}")
            
            if pet_notify_config and is_truthy_config(pet_notify_config.config_value):
                debug_print(f"Group {group_id} has pet notifications enabled")
                
                notification_data = {
                    'group_id': group_id,
                    'player_name': player_name,
                    'player_id': player_id,
                    'pet_name': pet_name,
                    'source': source,
                    'npc_name': npc_name,
                    'killcount': killcount,
                    'milestone': milestone,
                    'duplicate': duplicate,
                    'previously_owned': previously_owned,
                    'game_message': game_message,
                    'image_url': dl_path,
                    'item_id': pet_item_id,
                    'npc_id': npc_id,
                    'is_new_pet': is_new_pet
                }
                
                # Create XenForo entry (only once)
                # if not has_xf_entry:
                #     try:
                #         # Note: XenForo entry creation might need to be adapted for pets
                #         # For now, we'll skip it or create a generic entry
                #         debug_print("Skipping XenForo entry for pets (not implemented yet)")
                #         has_xf_entry = True
                #     except Exception as e:
                #         debug_print(f"Couldn't add pet to XenForo: {e}")
                #         app_logger.log(log_type="error", data=f"Couldn't add pet to XenForo: {e}", app_name="core", description="pet_processor")
                
                # Check for user DM settings
                if player and player.user:
                    user = session.query(User).filter(User.user_id == player.user_id).first()
                    if user and is_user_dm_enabled(session, user.user_id, 'dm_pets'):
                        debug_print(f"Creating DM notification for user {user.user_id}")
                        await create_notification('dm_pet', player_id, notification_data, group_id, existing_session=session if use_external_session else None)
                
                # Create group notification
                await create_notification('pet', player_id, notification_data, group_id, existing_session=session if use_external_session else None)
                debug_print(f"Created pet notification for group {group_id}")
    
    debug_print(f"=== PET PROCESSOR END ===")
    return existing_pet if existing_pet else (new_pet if is_new_pet and pet_item_id else None)

async def adventure_log_processor(adventure_log_data, external_session=None):
    debug_print("adventure_log_processor")
    print("Got adventure log data:", adventure_log_data)
    use_external_session = external_session is not None
    if use_external_session:
        db_session = external_session
    else:
        db_session = session

        player_name = adventure_log_data.get('player_name', adventure_log_data.get('player', None))
        player, authed, user_exists = await ensure_player_by_name_then_auth(db_session, player_name, account_hash, "")
        if not player:
            return
        player_id = player.player_id
        if not user_exists or not authed:
            return
        player_name = adventure_log_processor(adventure_log_data.get('player_name', adventure_log_data.get('player', None)))
        account_hash = adventure_log_data.get('acc_hash', adventure_log_data.get('account_hash', None))
        if adventure_log_data.get('adventure_log', None):
            print("Adventure log data decoded properly...")
            adventure_log = adventure_log_data.get('adventure_log', None)
            adventure_log = adventure_log.replace("[", "")
            adventure_log = adventure_log.replace("]", "")
            adventure_log = adventure_log.split(",")
            if len(adventure_log) > 0:
                try:
                    pb_content = adventure_log
                    personal_bests = pb_content.split("\n")
                    for pb in personal_bests:
                        boss_name, rest = pb.split(" - ")
                        team_size, time = rest.split(" : ")
                        boss_name = boss_name.strip()
                        team_size = team_size.strip()
                        boss_name, team_size, time = boss_name.replace("`", ""), team_size.replace("`", ""), time.replace("`", "")
                        time = time.strip()
                        real_boss_name, npc_id = get_true_boss_name(boss_name)
                        existing_pb = db_session.query(PersonalBestEntry).filter(PersonalBestEntry.player_id == player_id, PersonalBestEntry.npc_id == npc_id,
                                                                            PersonalBestEntry.team_size == team_size).first()
                        time_ms = convert_to_ms(time)
                        if existing_pb:
                            if time_ms < existing_pb.personal_best:
                                existing_pb.personal_best = time_ms
                                db_session.commit()
                        else:
                            new_pb = PersonalBestEntry(player_id=player_id, npc_id=npc_id, 
                                                    team_size=team_size, personal_best=time_ms, 
                                                    kill_time=time_ms, new_pb=True)
                            db_session.add(new_pb)
                            db_session.commit()
                
                except ValueError:
                    pet_list = adventure_log_data.get('pet_list', None)
                    pet_list = pet_list.replace("[", "")
                    pet_list = pet_list.replace("]", "")
                    pet_list = pet_list.split(",")
                    if len(pet_list) > 0:
                        for pet in pet_list:
                            pet = int(pet.strip())
                            item_object: ItemList = db_session.query(ItemList).filter(ItemList.item_id == pet).first()
                            if item_object:
                                player_pet = PlayerPet(player_id=player_id, item_id=item_object.item_id, pet_name=item_object.item_name)
                                try:
                                    db_session.add(player_pet)
                                    db_session.commit()
                                    print("Added a pet to the database for", player_name, account_hash, item_object.item_name, item_object.item_id)
                                except Exception as e:
                                    print("Couldn't add a pet to the database:", e)
                                    db_session.rollback()

# Cache for storing pending PB submissions to handle rapid submissions
# Structure: {player_name: {'submissions': [pb_data], 'timestamp': time.time()}}
toa_cache = {}

def check_player_and_clean_toa_cache(player_name):
    """Check if player has pending submissions in cache and clean expired entries"""
    current_time = time.time()
    
    # Clean expired entries (older than 10 seconds)
    expired_players = []
    for cached_player, cache_data in toa_cache.items():
        if current_time - cache_data['timestamp'] > 10:
            expired_players.append(cached_player)
    
    for player in expired_players:
        del toa_cache[player]
    
    # Return cached submissions for the player if they exist and are not expired
    if player_name in toa_cache:
        cache_data = toa_cache[player_name]
        if current_time - cache_data['timestamp'] <= 10:
            return cache_data['submissions']
        else:
            # Remove expired entry
            del toa_cache[player_name]
    
    return None

def add_to_toa_cache(player_name, pb_data):
    """Add a PB submission to the cache for potential deduplication"""
    current_time = time.time()
    
    if player_name not in toa_cache:
        toa_cache[player_name] = {
            'submissions': [],
            'timestamp': current_time
        }
    
    toa_cache[player_name]['submissions'].append(pb_data)
    toa_cache[player_name]['timestamp'] = current_time

def get_best_amascut_submission(submissions):
    """Select the submission with the largest team size from multiple Amascut/TOB submissions"""
    if not submissions:
        return None
    
    # Filter for Amascut or Theatre of Blood submissions
    tob_submissions = [
        sub for sub in submissions 
        if ('Amascut' in sub.get('npc_name', '') or 'Amascut' in sub.get('boss_name', '')) or
           ('Theatre of Blood' in sub.get('npc_name', '') or 'Theatre of Blood' in sub.get('boss_name', ''))
    ]
    
    if not tob_submissions:
        return submissions[0]  # Return first submission if no TOB submissions
    
    # Convert team_size to numeric value for proper comparison
    def get_team_size_numeric(team_size):
        if team_size == "Solo":
            return 1
        try:
            return int(team_size)
        except (ValueError, TypeError):
            return 1  # Default to 1 if conversion fails
    
    # Return the submission with the largest team size (numeric comparison)
    return max(tob_submissions, key=lambda x: get_team_size_numeric(x.get('team_size', 1)))

def clear_player_from_cache(player_name):
    """Remove a player from the cache after processing"""
    if player_name in toa_cache:
        del toa_cache[player_name]

async def delayed_amascut_processor(player_name, external_session=None):
    """Process TOB submissions after a delay to allow for multiple submissions"""
    # Wait 10 seconds to allow for additional submissions
    await asyncio.sleep(10)
    
    # Check if player still has cached submissions
    cached_submissions = check_player_and_clean_toa_cache(player_name)
    
    if cached_submissions:
        # Select the best submission (largest team size)
        best_submission = get_best_amascut_submission(cached_submissions)
        
        if best_submission:
            debug_print(f"Processing delayed TOB submission for {player_name} with team size: {best_submission.get('team_size', 1)}")
            
            # Clear the cache before processing to prevent infinite recursion
            clear_player_from_cache(player_name)
            
            # Process the best submission directly (bypass the caching logic)
            await process_amascut_submission_directly(best_submission, external_session)
        else:
            # No valid submissions found, clear cache
            clear_player_from_cache(player_name)
    else:
        debug_print(f"No cached submissions found for {player_name} after delay")

async def process_amascut_submission_directly(pb_data, external_session=None):
    """Process a TOB submission directly without caching logic"""
    debug_print(f"=== DIRECT TOB PROCESSOR START ===")
    debug_print(f"Raw PB data: {pb_data}")
    debug_print(f"External session provided: {external_session is not None}")
    
    session, use_external_session = select_session_and_flag(external_session)
    debug_print(f"Using external session: {use_external_session}")
    player_name = pb_data['player_name']
    account_hash = pb_data['acc_hash']
    boss_name = pb_data.get('npc_name', pb_data.get('boss_name', None))
    current_ms = pb_data.get('current_time_ms', pb_data.get('kill_time', 0))
    pb_ms = pb_data.get('personal_best_ms', pb_data.get('best_time', 0))
    pb_ms = convert_to_ms(pb_ms)
    current_ms = convert_to_ms(current_ms)
    if pb_ms == 0 and current_ms == 0:
        return ## No time was provided; ignoring this submission...
    team_size = pb_data.get('team_size', 1)
    is_personal_best = pb_data.get('is_new_pb', pb_data.get('is_pb', False))
    is_personal_best = True if is_personal_best == "true" else False
    time_ms = current_ms if current_ms < pb_ms and current_ms != 0 else (pb_ms if pb_ms != 0 else current_ms)
    auth_key = pb_data.get('auth_key', '')
    attachment_url = pb_data.get('attachment_url', None)
    attachment_type = pb_data.get('attachment_type', None)
    downloaded = pb_data.get('downloaded', False)
    image_url = pb_data.get('image_url', None)
    used_api = pb_data.get('used_api', False)
    unique_id = pb_data.get('guid', None)

    if not await ensure_can_create(session, unique_id, "pb"):
        print(f"Personal Best entry with Unique ID {unique_id} already exists in the database, aborting")
        return
    
    # Continue with the rest of the PB processing logic (same as pb_processor but without caching)
    player = None
    has_xf_entry = False
    dl_path = None
    npc_name = boss_name
    npc_id, npc_name = await ensure_npc_id_for_player(session, npc_name, player_list.get(player_name) or 0, player_name, use_external_session)
    if npc_id is None:
        return
    # Validate player
    player, authed, user_exists = await ensure_player_by_name_then_auth(session, player_name, account_hash, auth_key)
    if not player:
        return
    player_id = player.player_id
    if not user_exists or not authed:
        return
    pb_entry = session.query(PersonalBestEntry).filter(
        PersonalBestEntry.player_id == player_id,
        PersonalBestEntry.npc_id == npc_id,
        PersonalBestEntry.team_size == team_size
    ).first()
    old_time = None
    
    # Process image if available
    if is_personal_best:
        if attachment_url and not downloaded:
            try:
                print(f"Debug - pb attachment_type: '{attachment_type}'")
                file_extension = get_extension_from_content_type(attachment_type)
                print(f"Debug - pb file_extension after conversion: '{file_extension}'")
                file_name = f"pb_{player_id}_{boss_name.replace(' ', '_')}_{int(time.time())}"
                
                dl_path, external_url = await download_player_image(
                    submission_type="pb",
                    file_name=file_name,
                    player=player,
                    attachment_url=attachment_url,
                    file_extension=file_extension,
                    entry_id=pb_entry.id if pb_entry else 0,
                    entry_name=boss_name
                )
                
                # Use external_url for serving, not the local path
                if external_url:
                    pb_entry.image_url = external_url
                    session.commit()
            except Exception as e:
                app_logger.log(log_type="error", data=f"Couldn't download PB image: {e}", app_name="core", description="pb_processor")
        elif downloaded:
            dl_path = image_url
    if pb_entry:
        if pb_entry.personal_best > current_ms:
            old_time = pb_entry.personal_best
            pb_entry.personal_best = time_ms  
            pb_entry.new_pb=is_personal_best
            pb_entry.kill_time = current_ms
            pb_entry.date_added = datetime.now()
            pb_entry.image_url = dl_path if dl_path else ""
            is_personal_best = True
        else:
            is_personal_best = False
    else:
        pb_entry = PersonalBestEntry(
            player_id=player_id,
            npc_id=npc_id,
            team_size=team_size,
            new_pb=is_personal_best,
            personal_best=time_ms,
            kill_time=current_ms,
            date_added=datetime.now(),
            image_url=dl_path if dl_path else "",
            used_api=used_api,
            unique_id=unique_id
        )
        session.add(pb_entry)
        session.commit()
        session.refresh(pb_entry)
    
    session.commit()
    # Create notification if it's a new PB
    if is_personal_best:
        # Get player groups
        ## We need to determine what KC the player has received this PB at
        try:
            current_kc = await get_player_boss_kills(player_name, npc_name)  # int | 0 | None
            print("Got current KC:", current_kc)
            if current_kc >= 50:
                award_points_to_player(player_id=player_id, amount=20, source=f'New Personal Best ({convert_from_ms(time_ms)}) at {npc_name}', expires_in_days=60)
        except Exception as e:
            print("Couldn't get current KC:")
            print(e)
        print("Player found, getting groups")
        player_groups = get_player_groups_with_global(session, player)
        for group in player_groups:
            group_id = group.group_id
            print("Checking group: " + str(group))
            
            # Check if group has PB notifications enabled
            pb_notify_config = session.query(GroupConfiguration).filter(
                GroupConfiguration.group_id == group_id,
                GroupConfiguration.config_key == 'notify_pbs'
            ).first()
            print("PB notify config: " + str(pb_notify_config))
            if pb_notify_config and pb_notify_config.config_value.lower() == 'true' or int(pb_notify_config.config_value) == 1:
                notification_data = {
                    'player_name': player_name,
                    'player_id': player_id,
                    'pb_id': pb_entry.id,
                    'npc_id': npc_id,
                    'boss_name': boss_name,
                    'time_ms': time_ms,
                    'old_time_ms': old_time,
                    'team_size': team_size,
                    'kill_time_ms': current_ms,
                    'image_url': pb_entry.image_url
                }
                print("Creating notification")
                ## Check if we should send a notification for this npc
                await create_notification('pb', player_id, notification_data, group_id, existing_session=session if use_external_session else None  )
                # if not has_xf_entry:
                #     await create_xenforo_entry(drop=None, clog=None, personal_best=pb_entry, combat_achievement=None)
                #     has_xf_entry = True
                if player:
                    if player.user:
                        user = session.query(User).filter(User.user_id == player.user_id).first()
                        if user:
                            should_dm_cfg = session.query(UserConfiguration).filter(UserConfiguration.user_id == user.user_id,
                                                                                    UserConfiguration.config_key == 'dm_pbs').first()
                            if should_dm_cfg:
                                should_dm = should_dm_cfg.config_value
                                should_dm = str(should_dm).lower()
                                if should_dm == "true" or should_dm == "1":
                                    should_dm = True
                                else:
                                    should_dm = False
                                if should_dm:
                                    await create_notification('dm_pb', player_id, notification_data, group_id, existing_session=session if use_external_session else None)
                
    debug_print(f"=== DIRECT TOB PROCESSOR END ===")
    return pb_entry

async def pb_processor(pb_data, external_session=None):
    """
    Process a personal best submission and create notification entries if needed.
    
    Handles personal best submissions from the RuneLite plugin, including special
    logic for Theatre of Blood (Amascut) submissions which are cached and processed
    in batches to handle team submissions properly.
    
    Args:
        pb_data (dict): Personal best submission data including:
            - player_name: OSRS username
            - account_hash: Unique account identifier  
            - auth_key: Authentication key
            - npc_name: Boss/activity name
            - pb_type: Type of personal best (time, score, etc.)
            - pb_value: The personal best value achieved
            - team_size: Size of the team (for group activities)
            - unique_id: Unique identifier for deduplication
        external_session (Session, optional): Database session to use. Defaults to None.
        
    Returns:
        PersonalBestEntry: The created personal best entry, or None if processing failed
        
    Note:
        Special handling for Theatre of Blood submissions:
        - Submissions are cached for 10 seconds to collect team submissions
        - The best submission (largest team size) is selected for processing
        - This prevents duplicate notifications for team activities
    """
    debug_print(f"=== PB PROCESSOR START ===")
    debug_print(f"Raw PB data: {pb_data}")
    debug_print(f"External session provided: {external_session is not None}")
    
    session, use_external_session = select_session_and_flag(external_session)
    debug_print(f"Using external session: {use_external_session}")
    player_name = pb_data['player_name']
    account_hash = pb_data['acc_hash']
    boss_name = pb_data.get('npc_name', pb_data.get('boss_name', None))
    current_ms = pb_data.get('current_time_ms', pb_data.get('kill_time', 0))
    pb_ms = pb_data.get('personal_best_ms', pb_data.get('best_time', 0))
    pb_ms = convert_to_ms(pb_ms)
    current_ms = convert_to_ms(current_ms)
    if pb_ms == 0 and current_ms == 0:
        return ## No time was provided; ignoring this submission...
    team_size = pb_data.get('team_size', 1)
    is_personal_best = pb_data.get('is_new_pb', pb_data.get('is_pb', False))
    is_personal_best = True if is_personal_best == "true" else False
    time_ms = current_ms if current_ms < pb_ms and current_ms != 0 else (pb_ms if pb_ms != 0 else current_ms)
    auth_key = pb_data.get('auth_key', '')
    attachment_url = pb_data.get('attachment_url', None)
    attachment_type = pb_data.get('attachment_type', None)
    downloaded = pb_data.get('downloaded', False)
    image_url = pb_data.get('image_url', None)
    used_api = pb_data.get('used_api', False)
    unique_id = pb_data.get('guid', None)

    # Check if this is an Amascut or Theatre of Blood submission that might need caching
    is_tob_submission = ('Amascut' in (boss_name or '')) or ('Theatre of Blood' in (boss_name or ''))
    
    if is_tob_submission:
        # Check for existing cached submissions for this player
        cached_submissions = check_player_and_clean_toa_cache(player_name)
        
        if cached_submissions:
            # Add current submission to cache
            add_to_toa_cache(player_name, pb_data)
            print(f"TOB: Added submission to cache with team size: {team_size} (cached submissions exist)")
            
            # Always return early for TOB submissions - let delayed processor handle it
            return None
        else:
            # No cached submissions, add current one to cache and schedule delayed processing
            add_to_toa_cache(player_name, pb_data)
            print(f"TOB: Added submission to cache with team size: {team_size}")
            
            # Schedule delayed processing to wait for potential additional submissions
            asyncio.create_task(delayed_amascut_processor(player_name, external_session))
            return None

    if not await ensure_can_create(session, unique_id, "pb"):
        print(f"Personal Best entry with Unique ID {unique_id} already exists in the database, aborting")
        return
    
    # print(f"Extracted PB data - Player: {player_name}, Boss: {boss_name}, Team size: {team_size}")
    # print(f"Current time: {current_ms}ms, PB time: {pb_ms}ms, Final time: {time_ms}ms")
    # print(f"Is personal best: {is_personal_best}, Used API: {used_api}")
    # print(f"Account hash: {account_hash[:8]}... (truncated)")
    player = None
    has_xf_entry = False
    dl_path = None
    npc_name = boss_name
    npc_id, npc_name = await ensure_npc_id_for_player(session, npc_name, player_list.get(player_name) or 0, player_name, use_external_session)
    if npc_id is None:
        return
    # Validate player
    player, authed, user_exists = await ensure_player_by_name_then_auth(session, player_name, account_hash, auth_key)
    if not player:
        return
    player_id = player.player_id
    if not user_exists or not authed:
        return
    pb_entry = session.query(PersonalBestEntry).filter(
        PersonalBestEntry.player_id == player_id,
        PersonalBestEntry.npc_id == npc_id,
        PersonalBestEntry.team_size == team_size
    ).first()
    old_time = None
    
    
    
    # Process image if available
    if is_personal_best:
        if attachment_url and not downloaded:
            try:
                print(f"Debug - pb attachment_type: '{attachment_type}'")
                file_extension = get_extension_from_content_type(attachment_type)
                print(f"Debug - pb file_extension after conversion: '{file_extension}'")
                file_name = f"pb_{player_id}_{boss_name.replace(' ', '_')}_{int(time.time())}"
                
                dl_path, external_url = await download_player_image(
                    submission_type="pb",
                    file_name=file_name,
                    player=player,
                    attachment_url=attachment_url,
                    file_extension=file_extension,
                    entry_id=pb_entry.id if pb_entry else 0,
                    entry_name=boss_name
                )
                
                # Use external_url for serving, not the local path
                if external_url:
                    pb_entry.image_url = external_url
                    session.commit()
            except Exception as e:
                app_logger.log(log_type="error", data=f"Couldn't download PB image: {e}", app_name="core", description="pb_processor")
        elif downloaded:
            dl_path = image_url
    if pb_entry:
        if pb_entry.personal_best > current_ms:
            old_time = pb_entry.personal_best
            pb_entry.personal_best = time_ms  
            pb_entry.new_pb=is_personal_best
            pb_entry.kill_time = current_ms
            pb_entry.date_added = datetime.now()
            pb_entry.image_url = dl_path if dl_path else ""
            is_personal_best = True
        else:
            is_personal_best = False
    else:
        pb_entry = PersonalBestEntry(
            player_id=player_id,
            npc_id=npc_id,
            team_size=team_size,
            new_pb=is_personal_best,
            personal_best=time_ms,
            kill_time=current_ms,
            date_added=datetime.now(),
            image_url=dl_path if dl_path else "",
            used_api=used_api,
            unique_id=unique_id
        )
        session.add(pb_entry)
        session.commit()
        session.refresh(pb_entry)
    
    session.commit()
    #print("Committed PB entry - personal best: " + str(is_personal_best))
    # Create notification if it's a new PB
    if is_personal_best:
        #print("Is personal best, creating notification")
        # Get player groups
        ## We need to determine what KC the player has received this PB at
        try:
            current_kc = await get_player_boss_kills(player_name, npc_name)  # int | 0 | None
            print("Got current KC:", current_kc)
            if current_kc >= 50:
                award_points_to_player(player_id=player_id, amount=20, source=f'New Personal Best ({convert_from_ms(time_ms)}) at {npc_name}', expires_in_days=60)
        except Exception as e:
            print("Couldn't get current KC:")
            print(e)
        print("Player found, getting groups")
        player_groups = get_player_groups_with_global(session, player)
        for group in player_groups:
            group_id = group.group_id
            print("Checking group: " + str(group))
            
            # Check if group has PB notifications enabled
            pb_notify_config = session.query(GroupConfiguration).filter(
                GroupConfiguration.group_id == group_id,
                GroupConfiguration.config_key == 'notify_pbs'
            ).first()
            print("PB notify config: " + str(pb_notify_config))
            if pb_notify_config and pb_notify_config.config_value.lower() == 'true' or int(pb_notify_config.config_value) == 1:
                notification_data = {
                    'player_name': player_name,
                    'player_id': player_id,
                    'pb_id': pb_entry.id,
                    'npc_id': npc_id,
                    'boss_name': boss_name,
                    'time_ms': time_ms,
                    'old_time_ms': old_time,
                    'team_size': team_size,
                    'kill_time_ms': current_ms,
                    'image_url': pb_entry.image_url
                }
                print("Creating notification")
                ## Check if we should send a notification for this npc
                await create_notification('pb', player_id, notification_data, group_id, existing_session=session if use_external_session else None  )
                # if not has_xf_entry:
                #     await create_xenforo_entry(drop=None, clog=None, personal_best=pb_entry, combat_achievement=None)
                #     has_xf_entry = True
                if player:
                    if player.user:
                        user = session.query(User).filter(User.user_id == player.user_id).first()
                        if user:
                            should_dm_cfg = session.query(UserConfiguration).filter(UserConfiguration.user_id == user.user_id,
                                                                                    UserConfiguration.config_key == 'dm_pbs').first()
                            if should_dm_cfg:
                                should_dm = should_dm_cfg.config_value
                                should_dm = str(should_dm).lower()
                                if should_dm == "true" or should_dm == "1":
                                    should_dm = True
                                else:
                                    should_dm = False
                                if should_dm:
                                    await create_notification('dm_pb', player_id, notification_data, group_id, existing_session=session if use_external_session else None)
                
    debug_print(f"=== PB PROCESSOR END ===")
    return pb_entry

async def drop_processor(drop_data: RawDropData, external_session=None):
    """
    Process a drop submission and create notification entries if needed.
    
    This is the main function for processing drop submissions from the RuneLite plugin
    or API endpoints. It handles validation, authentication, value calculation, and
    notification creation for drop events.
    
    Args:
        drop_data (RawDropData): Container with drop submission data including:
            - player_name: OSRS username
            - account_hash: Unique account identifier
            - auth_key: Authentication key
            - item_name: Name of the dropped item
            - npc_name: Name of the NPC that dropped the item
            - quantity: Number of items dropped
            - value: Grand Exchange value (optional, will be calculated if missing)
            - image_url: URL to screenshot (optional)
            - unique_id: Unique identifier for deduplication (optional)
        external_session (Session, optional): Database session to use. Defaults to None.
        
    Returns:
        SubmissionResponse: Response object indicating success/failure and any messages
        
    Note:
        This function handles:
        - Player authentication and validation
        - Item and NPC resolution/creation
        - Drop deduplication using unique_id
        - Value calculation using Grand Exchange API
        - Group notification creation
        - Redis cache updates for leaderboards
        - Image processing for drop screenshots
    """
    debug_print(f"=== DROP PROCESSOR START ===")
    debug_print(f"Raw drop data: {drop_data}")
    debug_print(f"External session provided: {external_session is not None}")
    
    # Use provided session or create a new one
    session, use_external_session = select_session_and_flag(external_session)
    debug_print(f"Using external session: {use_external_session}")
    try:
        npc_name = drop_data.get('source', drop_data.get('npc_name', None))
        value = drop_data['value']
        item_id = drop_data.get('item_id', drop_data.get('id', None))
        item_name = drop_data.get('item_name', drop_data.get('item', None))
        quantity = drop_data['quantity']
        auth_key = drop_data.get('auth_key', None)
        player_name = drop_data.get('player_name', drop_data.get('player', None))
        account_hash = drop_data['acc_hash']
        kill_count = drop_data.get('kill_count', None)
        player_name = str(player_name).strip()
        account_hash = str(account_hash)
        guid = drop_data.get('guid', None)
        downloaded = drop_data.get('downloaded', False)
        image_url = drop_data.get('image_url', None)
        
        used_api = drop_data.get('used_api', False)
        if not await ensure_can_create(session, guid, "drop"):
            return
        debug_print(f"Extracted data - Player: {player_name}, Item: {item_name} (ID: {item_id}), NPC: {npc_name}")
        debug_print(f"Value: {value}, Quantity: {quantity}, Used API: {used_api}")
        debug_print(f"Account hash: {account_hash[:8]}... (truncated), Kill count: {kill_count}")
        debug_print(f"Ensuring item exists for drop...")
        item = await ensure_item_for_drop(session, item_id, item_name)
        if not item:
            debug_print(f"Item {item_name} not found in database, aborting")
            return SubmissionResponse(success=False, message=f"Item was not found in the database")
        item_id = item.item_id
        debug_print(f"Item validated - ID: {item_id}, Name: {item_name}")
        
        debug_print(f"Ensuring player and auth...")
        player, authed, user_exists = await ensure_player_and_auth(session, player_name, account_hash, auth_key)
        if not player:
            debug_print("Player not found in the database")
            return SubmissionResponse(success=False, message=f"Player {player_name} not found in the database")
        if not user_exists or not authed:
            debug_print(player_name + " failed auth check")
            return SubmissionResponse(success=False, message=f"Player {player_name} failed auth check")
        debug_print(f"Player validated - ID: {player.player_id}, Name: {player_name}, Authed: {authed}")
        #app_logger.log(log_type="drop_processor", data=f"Drop processor running on {quantity} x {item_name} for {player_name}", app_name="core", description="drop_processor")
        
        debug_print(f"Ensuring NPC ID for {npc_name}...")
        npc_id, npc_name = await ensure_npc_id_for_player(session, npc_name, player.player_id, player_name, use_external_session)
        if npc_id is None:
            debug_print(f"NPC ID could not be resolved for {npc_name}, aborting")
            return
        debug_print(f"NPC validated - ID: {npc_id}, Name: {npc_name}")
        
        player_id = player_list[player_name]
        item = redis_client.get(item_id)
        if not item:
            item = session.query(ItemList.item_id).filter(ItemList.item_id == item_id).first()
        if item:
            redis_client.set(item_id, item[0])
        else:
            # Create notification for new item
            notification_data = {
                'item_name': item_name,
                'player_name': player_name,
                'item_id': item_id,
                'npc_name': npc_name,
                'value': value
            }
            
            await create_notification('new_item', player_id, notification_data, existing_session=session if use_external_session else None)
            debug_print(f"Item not found...", item_id, item_name)
            return SubmissionResponse(success=False, message=f"Item {item_name} not found in the database")
        debug_print(f"Calculating drop value...")
        raw_drop_value = await get_true_item_value(item_name, int(value))
        drop_value = int(raw_drop_value) * int(quantity)
        debug_print(f"Drop value calculated - Raw: {raw_drop_value}, Total: {drop_value} ({quantity}x)")
        
        if drop_value > 1000000:
            debug_print(f"High value drop detected, verifying item/NPC combination...")
            async with osrs_api.create_client() as client:
                is_from_npc = await client.semantic.check_drop(item_name, npc_name)
            if not is_from_npc:
                debug_print(f"Verification failed: {item_name} is not from {npc_name}")
                return SubmissionResponse(success=False, message=f"Item {item_name} is not from NPC {npc_name}")
            debug_print(f"Item/NPC combination verified successfully")
        
        # Process attachment
        debug_print(f"Processing attachment data...")
        attachment_url, attachment_type = resolve_attachment_from_drop_data(drop_data)
        debug_print(f"Attachment resolved - URL: {attachment_url}, Type: {attachment_type}")
        #app_logger.log(log_type="drop_processor", data=f"Drop value: {drop_value}; Item ID: {item_id}; Player ID: {player_id}; NPC ID: {npc_id}; Attachment URL: {attachment_url}", app_name="core", description="drop_processor")
        
        # Create the drop in database
        debug_print(f"Creating drop object in database...")
        drop = await db.create_drop_object(
            item_id=item_id,
            player_id=player_id,
            date_received=datetime.now(),
            npc_id=npc_id,
            value=int(raw_drop_value),
            quantity=int(quantity),
            image_url=attachment_url if attachment_url else None,
            authed=authed,
            attachment_url=attachment_url,
            attachment_type=attachment_type,
            used_api=used_api,  ## Used to determine if the drop was created from the API or not
            unique_id=guid,
            existing_session=session if use_external_session else None
        )
        debug_print(f"Drop created successfully - Drop ID: {drop.drop_id if drop else 'None'}")
        #app_logger.log(log_type="drop_processor", data=f"Drop created: {drop} ({drop.drop_id})", app_name="core", description="drop_processor")
        
        if not drop:
            debug_print("Failed to create drop")
            return SubmissionResponse(success=False, message=f"Failed to create drop")
        try:
            debug_print("Updating player in redis...")
            redis_updates.add_to_player(player, drop)
            debug_print("Player redis update completed")
           # update_player_in_redis(player_id, session, force_update=False, batch_drops=[drop], from_submission=True)
        except Exception as e:
            debug_print(f"Error updating player in redis: {e}")
            session.rollback()
            return
        # Get player groups and check if notification is needed
        debug_print(f"Getting player groups for {player_name}...")
        player_groups = get_player_groups_with_global(session, player)
        debug_print(f"Player groups found: {[group.group_name for group in player_groups]}")
        #app_logger.log(log_type="drop_processor", data=f"{drop.drop_id}: Player groups: {player_groups}", app_name="core", description="drop_processor")
        sent_group_notifications = []
        debug_print(f"Processing notifications for {len(player_groups)} groups...")
        has_awarded_points = False
        for group in player_groups:
            group_id = group.group_id
            debug_print(f"Processing group: {group.group_name} (ID: {group_id})")
            # Get minimum value to notify for this group
            min_value_config = session.query(GroupConfiguration).filter(
                GroupConfiguration.group_id == group_id,
                GroupConfiguration.config_key == 'minimum_value_to_notify'
            ).first()
            
            
            min_value_to_notify = int(min_value_config.config_value) if min_value_config else 2500000
            debug_print(f"Group {group_id} minimum value to notify: {min_value_to_notify}")
            #app_logger.log(log_type="drop_processor", data=f"{drop.drop_id}: Checking group {group_id}'s minimum value to notify: ({min_value_to_notify})", app_name="core", description="drop_processor")
            should_send_stacks = session.query(GroupConfiguration).filter(
                GroupConfiguration.group_id == group_id,
                GroupConfiguration.config_key == 'send_stacks_of_items'
            ).first()
            send_stacks = False
            if should_send_stacks:
                if should_send_stacks.config_value == '1' or should_send_stacks.config_value == 'true':
                    send_stacks = True
            # Check if drop value exceeds minimum for notification
            debug_print(f"Checking notification criteria - Raw value: {raw_drop_value}, Drop value: {drop_value}, Send stacks: {send_stacks}")
            player_dm_sent = False
            if int(raw_drop_value) >= min_value_to_notify or (send_stacks == True and int(drop_value) > min_value_to_notify):
                debug_print(f"Notification criteria met for group {group_id}")
                #app_logger.log(log_type="drop_processor", data=f"{drop.drop_id}: Drop value ({drop_value}) exceeds minimum value to notify ({min_value_to_notify})", app_name="core", description="drop_processor")
                point_divisor = get_point_divisor()
                if group_id != 2 and has_awarded_points == False and int(drop_value) > point_divisor:
                    print(f"Awarding points to {player_name} for drop {item_name} from {npc_name}")
                    has_awarded_points = True
                    points_to_award = int(drop_value / point_divisor)
                    award_points_to_player(player_id=player_id, amount=points_to_award, source=f'Drop: {item_name} from {npc_name}', expires_in_days=60)
                # Create notification entry
                notification_data = {
                    'drop_id': drop.drop_id,
                    'item_name': item_name,
                    'npc_name': npc_name,
                    'value': value,
                    'quantity': quantity,
                    'total_value': drop_value,
                    'kill_count': kill_count,
                    'player_name': player_name,
                    'player_id': player_id,
                    'image_url': drop.image_url,
                    'attachment_type': attachment_type
                }
                if group_id > 2:
                    sent_group_notifications.append(group.group_name)
                    debug_print(f"Added {group.group_name} to notification list")
                
                if player and player_dm_sent == False:
                    if player.user:
                        user = session.query(User).filter(User.user_id == player.user_id).first()
                        if user and is_user_dm_enabled(session, user.user_id, 'dm_drops'):
                            debug_print(f"Creating DM notification for user {user.user_id}")
                            await create_notification('dm_drop', player_id, notification_data, group_id, existing_session=session if use_external_session else None)
                            player_dm_sent = True
                debug_print(f"Creating XenForo entry for drop {drop.drop_id}")
                #await create_xenforo_entry(drop=drop, clog=None, personal_best=None, combat_achievement=None)
                debug_print(f"Creating group notification for {player_name} in group {group_id}")
                await create_notification('drop', player_id, notification_data, group_id, existing_session=session if use_external_session else None)
                ## After all of this data is complete, we can check if the group should have their board instantly updated
                should_instantly_update = session.query(FeatureActivation).filter(FeatureActivation.group_id == group_id,
                                                                                FeatureActivation.feature_id == 2,
                                                                                FeatureActivation.status == 'active').first()
                if group_id == 2 or should_instantly_update:
                    if group_id not in last_board_updates:
                        last_board_updates[group_id] = datetime.now() - timedelta(seconds=10)
                    if last_board_updates[group_id] > datetime.now() - timedelta(seconds=10):
                        debug_print(f"Skipping group {group_id}: within 10 second window for instant update")
                        continue
                    last_board_updates[group_id] = datetime.now()
                    #print(f"Instantly updating group {group_id}'s board")
                    #await instantly_update_board(group_id, force=True)
            else:
                debug_print(f"Notification criteria NOT met for group {group_id} - skipping")
        # Commit the session regardless - external caller will handle transaction management
        if not use_external_session:
            debug_print(f"Committing session (we own it)")
            session.commit()
        else:
            debug_print(f"Not committing session (external session)")
        
        debug_print(f"Drop processor completed for {player_name}")
        if sent_group_notifications != []:
            if len(sent_group_notifications) == 1:
                group_name = sent_group_notifications[0]
            else:
                group_name = {', '.join(sent_group_notifications)}
            debug_print(f"Returning success with group notifications: {group_name}")
            debug_print(f"=== DROP PROCESSOR END (SUCCESS) ===")
            return SubmissionResponse(success=True, message=f"Drop created successfully", notice=f"Drop processed - a message has been sent to {group_name} for you")
        else:
            debug_print(f"Returning success without group notifications")
            debug_print(f"=== DROP PROCESSOR END (SUCCESS) ===")
            return SubmissionResponse(success=True, message=f"Drop created successfully")
        
    except Exception as e:
        # Roll back only if we own the session
        if not use_external_session:
            debug_print(f"Exception occurred, rolling back session: {e}")
            session.rollback()
        else:
            debug_print(f"Exception occurred with external session: {e}")
        debug_print(f"Error in drop_processor: {e}")
        debug_print(f"=== DROP PROCESSOR END (ERROR) ===")
        raise

async def create_player(player_name, account_hash, existing_session=None):
    
    """Create a player without Discord-specific functionality"""
    print("Called create_player")
    use_existing_session = existing_session is not None
    if use_existing_session:
        db_session = existing_session
    else:
        db_session = session
    account_hash = str(account_hash)
    if not account_hash or len(account_hash) < 5:
        debug_print("Account hash is too short, aborting")
        return False
    print("Checking if player exists again...")
    player = db_session.query(Player).filter(Player.player_name == player_name).first()
    
    if not player:
        wom_player, player_name, wom_player_id, log_slots = await check_user_by_username(player_name)
        account_hash = str(account_hash)
        print("Returned from wom check...")
        
        if not wom_player:
            print("No wom player found.")
            return None
        
        player: Player = db_session.query(Player).filter(Player.wom_id == wom_player_id).first()
        if not player:
            debug_print("Still no player after wom_id check")
            player: Player = db_session.query(Player).filter(Player.account_hash == account_hash).first()
        
        if player is not None:
            if normalize_player_display_equivalence(player_name) != normalize_player_display_equivalence(player.player_name):
                old_name = player.player_name
                player.player_name = player_name
                player.log_slots = log_slots
                db_session.commit()
                
                # Create name change notification
                notification_data = {
                    'player_name': player_name,
                    'player_id': player.player_id,
                    'old_name': old_name
                }
                if player:
                    if player.user:
                        user = db_session.query(User).filter(User.user_id == player.user_id).first()
                        if user:
                            should_dm_cfg = db_session.query(UserConfiguration).filter(UserConfiguration.user_id == user.user_id,
                                                                                    UserConfiguration.config_key == 'dm_account_changes').first()
                            if should_dm_cfg:
                                should_dm = should_dm_cfg.config_value
                                should_dm = str(should_dm).lower()
                                if should_dm == "true" or should_dm == "1":
                                    should_dm = True
                                else:
                                    should_dm = False
                                if should_dm:
                                    await create_notification('dm_name_change', player.player_id, notification_data, existing_session=db_session if use_existing_session else None)
                
                await create_notification('name_change', player.player_id, notification_data, existing_session=db_session if use_existing_session else None)
        else:
            debug_print(f"We found the player, updating their data")
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
            db_session.add(new_player)
            db_session.commit()
            
            player_list[player_name] = new_player.player_id
            app_logger.log(log_type="access", data=f"{player_name} has been created with ID {new_player.player_id} (hash: {account_hash}) ", app_name="core", description="create_player")
            
            # Create new player notification
            notification_data = {
                'player_name': player_name,
                'wom_id': wom_player_id,
                'player_id': new_player.player_id,
                'account_hash': account_hash
            }
            await create_notification('new_player', new_player.player_id, notification_data, existing_session=db_session if use_existing_session else None)
            
            return new_player
    else:
        stored_account_hash = player.account_hash
        if str(stored_account_hash) != account_hash:
            debug_print("Potential fake submission from" + player_name + " with a changed account hash!!")
        player_list[player_name] = player.player_id
    
    return player

stored_notifications = {}
recently_sent = []

async def create_notification(notification_type, player_id, data, group_id=None, existing_session=None):
    """Create a notification queue entry"""
    global stored_notifications
    debug_test = int(player_id) == 1
    if group_id is not None:
        if group_id not in stored_notifications:
            stored_notifications[group_id] = []
    else:
        if 0 not in stored_notifications:
            stored_notifications[0] = []
        group_id = 0
    if len(stored_notifications[group_id]) > 100:
        while len(stored_notifications[group_id]) > 100:
            stored_notifications[group_id].pop()
    use_existing_session = existing_session is not None
    if use_existing_session:
        db_session = existing_session
    else:
        db_session = session
    hashed_data = hashlib.sha256(json.dumps(data).encode()).hexdigest()
    if debug_test:
      await log_to_file(f"hashed data: {hashed_data}")
    if hashed_data in stored_notifications[group_id]:
        if debug_test:
          await log_to_file(f"Debug test: Notification already exists for group {group_id}, returning from create_notification without creation")
          await log_to_file(f"Existing hashed data: {stored_notifications[group_id]}")
        ## This group notification already got created ...
        return
    stored_notifications[group_id].append(hashed_data) # Add to the group's list
    notification = NotificationQueue(
        notification_type=notification_type,
        player_id=player_id,
        data=json.dumps(data),
        group_id=group_id if group_id != 0 else None,
        status='pending'
    )
    db_session.add(notification)
    # Only commit if we own the session
    if not use_existing_session:
        db_session.commit()
    if debug_test:
      await log_to_file(f"Debug test: Created notification for group {group_id}: {notification.id}")
    return notification.id


async def try_create_player(bot: interactions.Client, player_name, account_hash):
        account_hash = str(account_hash)
        if not account_hash or len(account_hash) < 5:
            return False # abort if no account hash was passed immediately
        #player_name = player_name.replace("-", " ")
        player = session.query(Player).filter(Player.player_name == player_name).first()
        
        if not player:
            print("Player not found in database, checking WOM...")
            wom_player, player_name, wom_player_id, log_slots = await check_user_by_username(player_name)
            account_hash = str(account_hash)
            if not wom_player:
                pass
                print("WOM player doesn't exist, and we can't update them/create them:", {player_name})
            elif not wom_player.latest_snapshot:
                print(f"Failed to find or create player via WOM: {player_name}. Aborting.")
                return 
            player: Player = session.query(Player).filter(Player.wom_id == wom_player_id).first()
            if not player:
                print("Player not found in database, checking account hash...")
                player: Player = session.query(Player).filter(Player.account_hash == account_hash).first()
            if player is not None:
                if normalize_player_display_equivalence(player_name) != normalize_player_display_equivalence(player.player_name):
                    old_name = player.player_name
                    player.player_name = player_name
                    player.log_slots = log_slots
                    session.commit()
                    if player.user:
                        user: User = player.user
                        user_discord_id = user.discord_id
                        if user_discord_id:
                            try:
                                user = await bot.fetch_user(user_id=user_discord_id)
                                if user:
                                    embed = interactions.Embed(title=f"Name change detected:",
                                                            description=f"Your account, {old_name}, has changed names to {player_name}.",
                                                            color="#00f0f0")
                                    embed.add_field(name=f"Is this a mistake?",
                                                    value=f"Reach out in [our discord](https://www.droptracker.io/discord)")
                                    embed.set_footer(global_footer)
                                    await user.send(f"Hey, <@{user.discord_id}>", embed=embed)
                            except Exception as e:
                                debug_print("Couldn't DM the user on a name change:" + str(e))
                    from utils.messages import name_change_message
                    await name_change_message(bot, player_name, player.player_id, old_name)
            else:
                debug_print("Player not found in database, creating new player..." + str(e))
                try:
                    overall = wom_player.latest_snapshot.data.skills.get('overall')
                    total_level = overall.level
                except Exception as e:
                    #print("Failed to get total level for player:", e)
                    total_level = 0
                new_player = Player(wom_id=wom_player_id, 
                                    player_name=player_name, 
                                    account_hash=account_hash, 
                                    total_level=total_level,
                                    log_slots=log_slots)
                session.add(new_player)
                from utils.messages import new_player_message
                await new_player_message(bot, player_name)
                session.commit()
                player_list[player_name] = new_player.player_id
                app_logger.log(log_type="access", data=f"{player_name} has been created with ID {new_player.player_id} (hash: {account_hash}) ", app_name="core", description="try_create_player")
                # await xf_api.try_create_xf_player(player_id=new_player.player_id,
                #                                   wom_id=new_player.wom_id,
                #                                   player_name=new_player.player_name,
                #                                   user_id=new_player.user_id,
                #                                   log_slots=0,
                #                                   total_level=total_level,
                #                                   xf_user_id=new_player.user.xf_user_id if new_player.user else None)
                return new_player
        else:
            stored_account_hash = player.account_hash
            if str(stored_account_hash) != account_hash:
                debug_print("Potential fake submission from " + player_name + " with a changed account hash!!")
            player_list[player_name] = player.player_id


async def log_to_file(data):
    ## Logs a string to a file, appending to the end of the file
    file_path = "data/logs/debug_test.log"
    try:
        with open(file_path, "a") as file:
            file.write(data + "\n")
    except Exception as e:
        debug_print("Couldn't log to file: " + str(e))
        app_logger.log(log_type="error", data=f"Couldn't log to file: {e}", app_name="core", description="log_to_file")
