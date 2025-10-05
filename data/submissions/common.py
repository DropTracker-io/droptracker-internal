"""Shared utilities and state for submissions processors.

This module centralizes common imports, shared caches, data classes,
and helper functions used by the various submission processors.

All functions/classes are exported with stable names to preserve
backward compatibility with the original `data.submissions` module.
"""

import asyncio
import hashlib
import json
import os
import time
from datetime import datetime, timedelta

from dotenv import load_dotenv

from db import (
    models,
    CombatAchievementEntry,
    Drop,
    FeatureActivation,
    NotifiedSubmission,
    PlayerPet,
    session,
    NpcList,
    Player,
    ItemList,
    PersonalBestEntry,
    CollectionLogEntry,
    User,
    Group,
    GroupConfiguration,
    UserConfiguration,
    NotificationQueue,
)
from db.ops import DatabaseOperations, associate_player_ids, get_point_divisor
from sqlalchemy import func, text
from sqlalchemy.engine import Row

from services import redis_updates
from services.points import award_points_to_player

from utils.ge_value import get_true_item_value
from utils import osrs_api
from utils.wiseoldman import (
    check_user_by_id,
    check_user_by_username,
    check_group_by_id,
    fetch_group_members,
    get_collections_logged,
    get_player_boss_kills,
    get_player_metric,
)
from utils.redis import RedisClient
from utils.download import download_player_image, download_image
from utils.format import (
    convert_to_ms,
    format_number,
    get_command_id,
    get_extension_from_content_type,
    get_true_boss_name,
    replace_placeholders,
    convert_from_ms,
    normalize_player_display_equivalence,
)
import interactions
from utils.logger import LoggerClient
from db.app_logger import AppLogger


load_dotenv()
debug_level = "false"
debug = debug_level != "false"


def debug_print(message, **kwargs):
    if debug:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}] DEBUG: {message}", **kwargs)


global_footer = os.getenv("DISCORD_MESSAGE_FOOTER")
redis_client = RedisClient()
db = DatabaseOperations()

last_channels_sent = []

app_logger = AppLogger()

# Caches
npc_list = {}
player_list = {}


class SubmissionResponse:
    """Response object for API submission endpoints.

    Attributes:
        success (bool): Whether the submission was processed successfully.
        message (str): Descriptive message about the submission result.
        notice (str | None): Optional additional notice or warning.
    """

    def __init__(self, success, message, notice=None):
        self.success = success
        self.message = message
        self.notice = notice


class RawDropData:
    """Container class for raw drop submission data."""

    def __init__(self) -> None:
        pass


def check_auth(player_name, account_hash, auth_key, external_session=None):
    """Authenticate a player against stored account hash.

    Returns:
        tuple[bool, bool]: (user_exists, authed)
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
            existing_player = db_session.query(Player).filter(Player.account_hash == account_hash).first()
            if existing_player:
                if (
                    normalize_player_display_equivalence(existing_player.player_name)
                    != normalize_player_display_equivalence(player_name)
                ):
                    existing_player.player_name = player_name
                    app_logger.log(
                        log_type="access",
                        data=f"Player {player_name} already exists with account hash {account_hash}, updating player name to {player_name}",
                        app_name="core",
                        description="check_auth",
                    )
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
    """Return (session, use_external_session_flag)."""

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
    """Ensure player exists, cache id, then auth. Returns (player, authed, user_exists)."""

    player = session.query(Player).filter(Player.account_hash == account_hash).first()
    if not player:
        player = await create_player(player_name, account_hash, existing_session=session)
        if not player:
            return None, False, False
    player_list[player_name] = player.player_id
    user_exists, authed = check_auth(player_name, account_hash, auth_key, session)
    return player, True, user_exists


unique_id_cache = {"clog": [], "drop": [], "pb": [], "ca": [], "pet": []}


async def ensure_can_create(session, unique_id, submission_type) -> bool:
    """Ensure no duplicate recent submission exists for a unique_id.

    Returns:
        bool: True if safe to create, False if duplicate exists.
    """

    if unique_id in unique_id_cache[submission_type]:
        return False
    unique_id_cache[submission_type].append(unique_id)
    if len(unique_id_cache[submission_type]) > 1000:
        unique_id_cache[submission_type].pop(0)
    match submission_type:
        case "clog":
            existing_entry = session.query(CollectionLogEntry).filter(
                CollectionLogEntry.unique_id == unique_id,
                CollectionLogEntry.date_added > datetime.now() - timedelta(hours=1),
            ).first()
            return False if existing_entry is not None else True
        case "drop":
            existing_entry = session.query(Drop).filter(
                Drop.unique_id == unique_id,
                Drop.used_api == True,
                Drop.date_added > datetime.now() - timedelta(hours=1),
            ).first()
            return False if existing_entry is not None else True
        case "pb":
            existing_entry = session.query(PersonalBestEntry).filter(
                PersonalBestEntry.unique_id == unique_id,
                PersonalBestEntry.date_added > datetime.now() - timedelta(hours=1),
            ).first()
            return False if existing_entry is not None else True
        case "ca":
            existing_entry = session.query(CombatAchievementEntry).filter(
                CombatAchievementEntry.unique_id == unique_id,
                CombatAchievementEntry.date_added > datetime.now() - timedelta(hours=1),
            ).first()
            return False if existing_entry is not None else True
        case "pet":
            existing_entry = session.query(PlayerPet).filter(
                PlayerPet.unique_id == unique_id,
                PlayerPet.date_added > datetime.now() - timedelta(hours=1),
            ).first()
            return False if existing_entry is not None else True


async def ensure_npc_id_for_player(session, npc_name, player_id, player_name, use_external_session):
    """Resolve npc_id using cache, DB, or create via external API, else queue notification."""

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
                level_value = int(level_value)
            except Exception:
                return 14704, npc_name
            npc_name = re.sub(r"\(\s*Level\s*:??\s*(\d+)\s*\)", r"(Level \1)", npc_name, flags=re.IGNORECASE)
            print("Parsed doom's name:", npc_name, "Level:", level_value)
            return (14707 + level_value), npc_name
        return 14707, npc_name
    npc_row = session.query(NpcList.npc_id).filter(NpcList.npc_name == npc_name).first()
    if npc_row:
        npc_list[npc_name] = npc_row.npc_id
        return npc_row.npc_id, npc_name
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
    notification_data = {"npc_name": npc_name, "player_name": player_name, "player_id": player_id}
    await create_notification(
        "new_npc",
        player_id,
        notification_data,
        existing_session=session if use_external_session else None,
    )
    return None, npc_name


def resolve_attachment_from_drop_data(drop_data):
    """Return (attachment_url, attachment_type) based on drop_data."""

    downloaded = drop_data.get("downloaded", False)
    image_url = drop_data.get("image_url", None)
    if downloaded:
        return image_url, "downloaded"
    if drop_data.get("attachment_type", None) is not None:
        return drop_data.get("attachment_url", None), drop_data.get("attachment_type", None)
    return image_url, None


def get_player_groups_with_global(session, player: Player):
    """Fetch groups via association table, ensure global group membership."""

    global_group = session.query(Group).filter(Group.group_id == 2).first()
    player_gids = session.execute(
        text("SELECT group_id FROM user_group_association WHERE player_id = :player_id"),
        {"player_id": player.player_id},
    ).all()
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
    return v == "true" or v == "1"


def get_group_drop_notify_settings(session, group_id):
    """Return (min_value_to_notify:int, send_stacks:bool)."""

    min_value_config = (
        session.query(GroupConfiguration)
        .filter(
            GroupConfiguration.group_id == group_id,
            GroupConfiguration.config_key == "minimum_value_to_notify",
        )
        .first()
    )
    min_value_to_notify = int(min_value_config.config_value) if min_value_config else 2500000
    should_send_stacks = (
        session.query(GroupConfiguration)
        .filter(
            GroupConfiguration.group_id == group_id,
            GroupConfiguration.config_key == "send_stacks_of_items",
        )
        .first()
    )
    send_stacks = is_truthy_config(should_send_stacks.config_value) if should_send_stacks else False
    return min_value_to_notify, send_stacks


def is_user_dm_enabled(session, user_id, key):
    cfg = (
        session.query(UserConfiguration)
        .filter(UserConfiguration.user_id == user_id, UserConfiguration.config_key == key)
        .first()
    )
    return is_truthy_config(cfg.config_value) if cfg else False


async def ensure_item_by_name(session, item_name):
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
    player = None
    if player_name:
        player = session.query(Player).filter(Player.player_name.ilike(player_name)).first()
        if player and player.player_name != player_name:
            if player.account_hash == account_hash:
                player.player_name = player_name
                session.commit()
    if not player:
        player = await create_player(player_name, account_hash, existing_session=session)
        if not player:
            return None, False, False
    player_list[player_name] = player.player_id
    user_exists, authed = check_auth(player_name, account_hash, auth_key, session)
    return player, authed, user_exists


stored_notifications = {}
recently_sent = []


async def create_notification(notification_type, player_id, data, group_id=None, existing_session=None):
    """Create a notification queue entry."""

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
            await log_to_file(
                f"Debug test: Notification already exists for group {group_id}, returning from create_notification without creation"
            )
            await log_to_file(f"Existing hashed data: {stored_notifications[group_id]}")
        return
    stored_notifications[group_id].append(hashed_data)
    notification = NotificationQueue(
        notification_type=notification_type,
        player_id=player_id,
        data=json.dumps(data),
        group_id=group_id if group_id != 0 else None,
        status="pending",
    )
    db_session.add(notification)
    if not use_existing_session:
        db_session.commit()
    if debug_test:
        await log_to_file(f"Debug test: Created notification for group {group_id}: {notification.id}")
    return notification.id


async def create_player(player_name, account_hash, existing_session=None):
    """Create a player without Discord-specific functionality."""

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

        player = db_session.query(Player).filter(Player.wom_id == wom_player_id).first()
        if not player:
            debug_print("Still no player after wom_id check")
            player = db_session.query(Player).filter(Player.account_hash == account_hash).first()

        if player is not None:
            if normalize_player_display_equivalence(player_name) != normalize_player_display_equivalence(
                player.player_name
            ):
                old_name = player.player_name
                player.player_name = player_name
                player.log_slots = log_slots
                db_session.commit()

                notification_data = {"player_name": player_name, "player_id": player.player_id, "old_name": old_name}
                if player:
                    if player.user:
                        user = db_session.query(User).filter(User.user_id == player.user_id).first()
                        if user:
                            should_dm_cfg = (
                                db_session.query(UserConfiguration)
                                .filter(
                                    UserConfiguration.user_id == user.user_id,
                                    UserConfiguration.config_key == "dm_account_changes",
                                )
                                .first()
                            )
                            if should_dm_cfg:
                                should_dm = str(should_dm_cfg.config_value).lower()
                                should_dm = True if should_dm in ("true", "1") else False
                                if should_dm:
                                    await create_notification(
                                        "dm_name_change",
                                        player.player_id,
                                        notification_data,
                                        existing_session=db_session if use_existing_session else None,
                                    )

                await create_notification(
                    "name_change",
                    player.player_id,
                    notification_data,
                    existing_session=db_session if use_existing_session else None,
                )
        else:
            debug_print(f"We found the player, updating their data")
            try:
                overall = wom_player.latest_snapshot.data.skills.get("overall")
                total_level = overall.level
            except Exception:
                total_level = 0

            new_player = Player(
                wom_id=wom_player_id,
                player_name=player_name,
                account_hash=account_hash,
                total_level=total_level,
                log_slots=log_slots,
            )
            db_session.add(new_player)
            db_session.commit()

            player_list[player_name] = new_player.player_id
            app_logger.log(
                log_type="access",
                data=f"{player_name} has been created with ID {new_player.player_id} (hash: {account_hash}) ",
                app_name="core",
                description="create_player",
            )

            notification_data = {
                "player_name": player_name,
                "wom_id": wom_player_id,
                "player_id": new_player.player_id,
                "account_hash": account_hash,
            }
            await create_notification(
                "new_player",
                new_player.player_id,
                notification_data,
                existing_session=db_session if use_existing_session else None,
            )

            return new_player
    else:
        stored_account_hash = player.account_hash
        if str(stored_account_hash) != account_hash:
            debug_print("Potential fake submission from" + player_name + " with a changed account hash!!")
        player_list[player_name] = player.player_id

    return player


async def try_create_player(bot: interactions.Client, player_name, account_hash):
    account_hash = str(account_hash)
    if not account_hash or len(account_hash) < 5:
        return False
    player = session.query(Player).filter(Player.player_name == player_name).first()

    if not player:
        print("Player not found in database, checking WOM...")
        wom_player, player_name, wom_player_id, log_slots = await check_user_by_username(player_name)
        account_hash = str(account_hash)
        if not wom_player:
            print("WOM player doesn't exist, and we can't update them/create them:", {player_name})
        elif not wom_player.latest_snapshot:
            print(f"Failed to find or create player via WOM: {player_name}. Aborting.")
            return
        player = session.query(Player).filter(Player.wom_id == wom_player_id).first()
        if not player:
            print("Player not found in database, checking account hash...")
            player = session.query(Player).filter(Player.account_hash == account_hash).first()
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
                                embed = interactions.Embed(
                                    title=f"Name change detected:",
                                    description=f"Your account, {old_name}, has changed names to {player_name}.",
                                    color="#00f0f0",
                                )
                                embed.add_field(
                                    name=f"Is this a mistake?",
                                    value=f"Reach out in [our discord](https://www.droptracker.io/discord)",
                                )
                                embed.set_footer(global_footer)
                                await user.send(f"Hey, <@{user.discord_id}>", embed=embed)
                        except Exception as e:
                            debug_print("Couldn't DM the user on a name change:" + str(e))
                from utils.messages import name_change_message

                await name_change_message(bot, player_name, player.player_id, old_name)
        else:
            debug_print("Player not found in database, creating new player..." + str(e))
            try:
                overall = wom_player.latest_snapshot.data.skills.get("overall")
                total_level = overall.level
            except Exception:
                total_level = 0
            new_player = Player(
                wom_id=wom_player_id,
                player_name=player_name,
                account_hash=account_hash,
                total_level=total_level,
                log_slots=log_slots,
            )
            session.add(new_player)
            from utils.messages import new_player_message

            await new_player_message(bot, player_name)
            session.commit()
            player_list[player_name] = new_player.player_id
            app_logger.log(
                log_type="access",
                data=f"{player_name} has been created with ID {new_player.player_id} (hash: {account_hash}) ",
                app_name="core",
                description="try_create_player",
            )
            return new_player
    else:
        stored_account_hash = player.account_hash
        if str(stored_account_hash) != account_hash:
            debug_print("Potential fake submission from " + player_name + " with a changed account hash!!")
        player_list[player_name] = player.player_id


async def log_to_file(data):
    file_path = "data/logs/debug_test.log"
    try:
        with open(file_path, "a") as file:
            file.write(data + "\n")
    except Exception as e:
        debug_print("Couldn't log to file: " + str(e))
        app_logger.log(log_type="error", data=f"Couldn't log to file: {e}", app_name="core", description="log_to_file")


