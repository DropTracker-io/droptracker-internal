import os
import asyncio
import httpx
from asynciolimiter import Limiter
from dotenv import load_dotenv
from db import Player, Session, session, models

import wom
from wom import Err, Result
from utils.format import normalize_player_display_equivalence
from typing import Optional
load_dotenv()

rate_limit = 100 / 65  # This calculates the rate as 100 requests per 65 seconds
limiter = Limiter(rate_limit)  # Create a Limiter instance

# Fetch the WOM_API_KEY from environment variables
WOM_API_KEY = os.getenv("WOM_API_KEY")

# Initialize the WOM Client with API key and user agent
client = wom.Client(
    WOM_API_KEY,
    user_agent="@joelhalen"
)

async def check_user_by_username(username: str):
    """ Check a user in the WiseOldMan database, returning their "player" object,
        their WOM ID, and their displayName.
    """
    # TODO -- only grab necessary info and parse it before returning the full player obj?
    await limiter.wait()
    await client.start()  # Initialize the client (if required by the `wom` library)
    try:
        result = await client.players.get_details(username=username)
        # Add debug logging
        try:
            if result.is_ok:
                player = result.unwrap()
                if player is None:
                    return None, None, None
                log_slots = 0
                snapshot_data = None
                snapshot = getattr(player, "latest_snapshot", None)
                if snapshot:
                    snapshot_data = getattr(snapshot, "data", None)
                else:
                    log_slots = -1
                if snapshot_data:
                    activities = getattr(snapshot_data, "activities", {})
                    for activity_name, activity_obj in activities.items():
                        activity_name_str = str(activity_name).split(".")[-1].lower()
                        score = getattr(activity_obj, "score", -1)
                        if activity_name_str == "collections_logged":
                            log_slots = score
                return player, player.username, player.id, log_slots
        except Exception as e:
            error = result.unwrap_err()
            if isinstance(error, Err):
                pass
        else:
            error = result.unwrap_err()
            if isinstance(error, Err):
                pass
            # Try update if get fails
            try:
                result = await client.players.update_player(username=username)
            except Exception as e:
                print("Error updating player:", e)
                pass
            # Add debug logging
            if not result.is_ok:
                print(f"Update player failed for {username}. Status: {result.status_code}")
                pass
            if result.is_ok:
                player = result.unwrap()
                
                if player is None:
                    print(f"Got empty player object after update for {username}")
                    return None, None, None
                print("Got player object after update for", username + ":", player)
                player_id = player.id
                player_name = player.username
                snapshot = getattr(player, "latest_snapshot", None)
                log_slots = 0
                snapshot_data = None
                if snapshot:
                    snapshot_data = getattr(snapshot, "data", None)
                else:
                    log_slots = -1
                if snapshot_data:
                    activities = getattr(snapshot_data, "activities", {})
                    for activity_name, activity_obj in activities.items():
                        activity_name_str = str(activity_name).split(".")[-1].lower()
                        score = getattr(activity_obj, "score", -1)
                        if activity_name_str == "collections_logged":
                            log_slots = score
                else:
                    return 0
                return player, str(player_name), str(player_id), log_slots
            else:
                print("Result is not ok, returning None")
                return None, None, None, -1
    except Exception as e:
        print(f"Error checking user {username}: {str(e)}")
        return None, None, None, -1

async def check_user_by_id(uid: int):
    """ Check a user in the WiseOldMan database, returning their "player" object,
        their WOM ID, and their displayName.
    """
    await client.start()  # Initialize the client (if required by the `wom` library)

    await limiter.wait()

    try:
        result = await client.players.get_details_by_id(player_id=uid)
        if result.is_ok:
            player = result.unwrap()
            player_id = player.player.id
            player_name = player.player.display_name
            return player, str(player_name), str(player_id)
        else:
            # Handle the case where the request failed
            return None, None, None
    finally:
        pass

async def check_group_by_id(wom_group_id: int):
    """ Searches for a group on WiseOldMan by a passed group ID 
        Returns group_name, member_count and members (list)    
    """
    wom_id = str(wom_group_id)
    await client.start()
    await limiter.wait()
    try:
        result = await client.groups.get_details(id=wom_id)
        if result.is_ok:
            details = result.unwrap()
            members = details.memberships
            member_count = details.group.member_count
            group_name = details.group.name
            return group_name, member_count, members
        else:
            return None, None, None
    finally:
        pass

async def fetch_group_members(wom_group_id: int, session_to_use = None):
    """ 
    Returns a list of WiseOldMan Player IDs 
    for members of a specified group 
    """
    #print("Fetching group members for ID:", wom_group_id)
    user_list = []
    if session_to_use is not None:
        session = session_to_use
    else:
        session = models.session
    
    if wom_group_id == 1:
        # Fetch all player WOM IDs from the database directly
        players = session.query(Player.wom_id).all()
        # Unpack the list of tuples returned by SQLAlchemy
        user_list = [player.wom_id for player in players] 
        return user_list
    await client.start()
    await limiter.wait()
    try:
        result = await client.groups.get_details(wom_group_id)
        if result.is_ok:
            details = result.unwrap()
            members = details.memberships
            name = details.name
            #print(f"Group name: {name}")
            for member in members:
                player_name = member.player.display_name
                existing_player = session.query(Player).filter(Player.wom_id == member.player_id).first()
                if existing_player:
                    old_name = existing_player.player_name or ""
                    new_name = player_name or ""
                    # Only update if the names differ beyond hyphen/underscore vs space changes
                    if normalize_player_display_equivalence(old_name) != normalize_player_display_equivalence(new_name):
                        if old_name != new_name:
                            print(f"Updated player name for {old_name} to {new_name}")
                            existing_player.player_name = new_name
                            session.commit()
                user_list.append(member.player_id)
            return user_list
        else:
            return []
    except Exception as e:
        print("Couldn't find WOM group members... Error:", e)
        return []

async def get_collections_logged(username: str):
    """
    Returns an integer representation of the number of collection 
    log slots a player has unlocked according to WiseOldMan
    """
    await client.start()
    await limiter.wait()
    player_data = await client.players.get_details(username=username)
    if player_data.is_ok:
        details = player_data.unwrap()
        snapshot = getattr(details, "latest_snapshot", None)
        if snapshot:
            snapshot_data = getattr(snapshot, "data", None)
        else:
            return -1
        if snapshot_data:
            activities = getattr(snapshot_data, "activities", {})
            for activity_name, activity_obj in activities.items():
                activity_name_str = str(activity_name).split(".")[-1].lower()
                score = getattr(activity_obj, "score", -1)
                if activity_name_str == "collections_logged":
                    return score
        else:
            return 0
    else:
        return -1
    
def get_player_total_kills(wom_id: int):
    loop = asyncio.get_event_loop()
    if loop.is_running():
        future = asyncio.run_coroutine_threadsafe(get_player_total_kills(wom_id), loop)
        return future.result()
    else:
        return loop.run_until_complete(get_player_total_kills(wom_id))
    
async def get_player_total_kills(wom_id: int):
    await client.start()
    await limiter.wait()
    player_data = await client.players.get_details_by_id(player_id=wom_id)
    if player_data.is_ok:
        details = player_data.unwrap()
        snapshot = getattr(details, "latest_snapshot", None)
        if snapshot is not None:
            snapshot_data = getattr(snapshot, "data", None)
            if snapshot_data is not None:
                bosses = getattr(snapshot_data, "bosses", {})
                for boss_name, boss_obj in bosses.items():
                    kills = getattr(boss_obj, "kills", -1)
                    if kills > 0:
                        return kills

def get_player_boss_kills_sync(username: str, boss_metric: str) -> Optional[int]:
    """
    Synchronous helper to fetch boss kill count by username.
    Returns int kills if available, 0 if boss not found or non-positive, or None on error.
    """
    loop = asyncio.get_event_loop()
    if loop.is_running():
        future = asyncio.run_coroutine_threadsafe(get_player_boss_kills(username, boss_metric), loop)
        return future.result()
    else:
        return loop.run_until_complete(get_player_boss_kills(username, boss_metric))

async def get_player_boss_kills(username: str, boss_metric: str) -> Optional[int]:
    """
    Return the kill count (int) for the specified boss metric for a given username.
    - Returns 0 if the boss is present but has no recorded kills or not found in the snapshot.
    - Returns None if data cannot be retrieved (API error or missing snapshot data).
    """
    await client.start()
    await limiter.wait()
    try:
        player_data: Result = await client.players.get_details(username=username)
        return await _extract_boss_kills_from_player_result(player_data, boss_metric)
    except Exception:
        return None

async def get_player_boss_kills_by_id(wom_id: int, boss_metric: str) -> Optional[int]:
    """
    Return the kill count (int) for the specified boss metric for a given WOM player id.
    - Returns 0 if the boss is present but has no recorded kills or not found in the snapshot.
    - Returns None if data cannot be retrieved (API error or missing snapshot data).
    """
    await client.start()
    await limiter.wait()
    try:
        player_data: Result = await client.players.get_details_by_id(wom_id)
        return await _extract_boss_kills_from_player_result(player_data, boss_metric)
    except Exception:
        return None

async def _extract_boss_kills_from_player_result(player_data: Result, boss_metric: str) -> Optional[int]:
    """
    Internal helper to normalize boss metric name and extract kills from a player Result.
    Returns int kills, 0 if not found/non-positive, or None if data unavailable.
    """
    normalized_target = (
        boss_metric.strip().lower().replace(" ", "_").replace("-", "_").replace("'", "")
    )
    if not player_data or not getattr(player_data, "is_ok", False):
        return None
    details = player_data.unwrap()
    snapshot = getattr(details, "latest_snapshot", None)
    if not snapshot:
        return None
    snapshot_data = getattr(snapshot, "data", None)
    if not snapshot_data:
        return None
    bosses = getattr(snapshot_data, "bosses", {}) or {}
    # Iterate bosses and match normalized metric key
    for boss_name, boss_obj in bosses.items():
        boss_key = str(boss_name).split(".")[-1].lower()
        if boss_key == normalized_target:
            kills = getattr(boss_obj, "kills", -1)
            try:
                kills_int = int(kills)
            except Exception:
                return 0
            return kills_int if kills_int > 0 else 0
    # If boss key didn't match any entry, return 0 per spec
    return 0

def get_player_metric_sync(username: str, metric_name: str):
    """
    Returns an integer representation of a player's metric according to WiseOldMan
    using the existing event loop
    """
    loop = asyncio.get_event_loop()
    if loop.is_running():
        # Create a future in the running loop
        future = asyncio.run_coroutine_threadsafe(get_player_metric(username, metric_name), loop)
        return future.result()  # This blocks until the result is available
    else:
        # If no loop is running, we can use loop.run_until_complete
        return loop.run_until_complete(get_player_metric(username, metric_name))

async def get_player_metric_by_id(wom_id: int, metric_name: str):
    """
    Returns an integer representation of a player's metric according to WiseOldMan
    """
    await client.start()
    await limiter.wait()
    player_data = await client.players.get_details_by_id(wom_id)
    return await _get_player_metric(player_data, metric_name)

async def _get_player_metric(player_data: Result, metric_name: str):
    metric_name = metric_name.replace(" ", "_").replace("'", "")
    if player_data.is_ok:
        details = player_data.unwrap()
        snapshot = getattr(details, "latest_snapshot", None)
        player_info = {
            "id": getattr(details, "id", None),
            "username": getattr(details, "username", None),
            "display_name": getattr(details, "display_name", None),
            "type": str(getattr(details, "type", None)),
            "build": str(getattr(details, "build", None)),
            "status": str(getattr(details, "status", None)),
            "combat_level": getattr(details, "combat_level", None),
            "exp": getattr(details, "exp", None),
            "ehp": getattr(details, "ehp", None),
            "ehb": getattr(details, "ehb", None),
            "ttm": getattr(details, "ttm", None),
            "tt200m": getattr(details, "tt200m", None),
            "registered_at": str(getattr(details, "registered_at", None)),
            "updated_at": str(getattr(details, "updated_at", None)),
            "last_changed_at": str(getattr(details, "last_changed_at", None))
        }
        if metric_name in player_info:
            return player_info[metric_name]
        skills_data = {}
        snapshot = getattr(details, "latest_snapshot", None)
        if snapshot:
            snapshot_data = getattr(snapshot, "data", None)
            if snapshot_data:
                skills = getattr(snapshot_data, "skills", {})
                for skill_name, skill_obj in skills.items():
                    skill_name_str = str(skill_name).split(".")[-1].lower()
                    skills_data[skill_name_str] = {
                        "level": getattr(skill_obj, "level", 0),
                        "experience": getattr(skill_obj, "experience", 0),
                        "rank": getattr(skill_obj, "rank", 0),
                        "ehp": getattr(skill_obj, "ehp", 0)
                    }
            if metric_name in skills_data:
                return skills_data[metric_name]
        boss_data = {}
        if snapshot and snapshot_data:
            bosses = getattr(snapshot_data, "bosses", {})
            print("Got bosses: " + str(bosses))
            for boss_name, boss_obj in bosses.items():
                kills = getattr(boss_obj, "kills", -1)
                if kills > 0:
                    boss_name_str = str(boss_name).split(".")[-1].lower()
                    boss_data[boss_name_str] = {
                        "kills": kills,
                        "rank": getattr(boss_obj, "rank", 0),
                        "ehb": getattr(boss_obj, "ehb", 0)
                    }
        
        if metric_name.lower() in [boss.lower() for boss in boss_data]:
            boss_data_obj = boss_data[metric_name.lower()]
            return {"kills": boss_data_obj["kills"]}
            # Extract activity data - include all activities
        activity_data = {}
        if snapshot and snapshot_data:
            activities = getattr(snapshot_data, "activities", {})
            for activity_name, activity_obj in activities.items():
                activity_name_str = str(activity_name).split(".")[-1].lower()
                score = getattr(activity_obj, "score", -1)
                activity_data[activity_name_str] = {
                    "score": score,
                    "rank": getattr(activity_obj, "rank", 0)
                }
        if metric_name in activity_data:
            return activity_data[metric_name]
        computed_data = {}
        if snapshot and snapshot_data:
            computed = getattr(snapshot_data, "computed", {})
            for metric_name, metric_obj in computed.items():
                metric_name_str = str(metric_name).split(".")[-1].lower()
                computed_data[metric_name_str] = {
                    "value": getattr(metric_obj, "value", 0),
                    "rank": getattr(metric_obj, "rank", 0)
                }
        if metric_name in computed_data:
            return computed_data[metric_name]
        else:
            return -1
    return -1

async def get_player_metric(username: str, metric_name: str):
    """
    Returns an integer representation of a player's metric according to WiseOldMan
    """
    await client.start()
    await limiter.wait()
    player_data = await client.players.get_details(username=username)
    return await _get_player_metric(player_data, metric_name)

async def get_player_wom_data(username: str):
    """
    Returns a player object from WiseOldMan
    """
    await client.start()
    await limiter.wait()
    player_data = await client.players.get_details(username=username)
    return player_data

async def get_player_all_skills(username: str):
    """
    Returns all skills and their experience points for a player according to WiseOldMan
    Returns a dictionary with skill names as keys and experience points as values
    """
    await client.start()
    await limiter.wait()
    player_data = await client.players.get_details(username=username)
    
    if player_data.is_ok:
        details = player_data.unwrap()
        snapshot = getattr(details, "latest_snapshot", None)
        
        if snapshot:
            snapshot_data = getattr(snapshot, "data", None)
            if snapshot_data:
                skills = getattr(snapshot_data, "skills", {})
                skills_data = []
                
                for skill_name, skill_obj in skills.items():
                    skill_name_str = str(skill_name).split(".")[-1].lower()
                    skills_data.append({
                        f"{skill_name_str}": getattr(skill_obj, "experience", 0)
                    })
                
                return skills_data
    
    return {}

async def get_player_all_skills_by_id(wom_id: int):
    """
    Returns all skills and their experience points for a player by WOM ID
    Returns a dictionary with skill names as keys and experience points as values
    """
    await client.start()
    await limiter.wait()
    player_data = await client.players.get_details_by_id(wom_id)
    
    if player_data.is_ok:
        details = player_data.unwrap()
        snapshot = getattr(details, "latest_snapshot", None)
        
        if snapshot:
            snapshot_data = getattr(snapshot, "data", None)
            if snapshot_data:
                skills = getattr(snapshot_data, "skills", {})
                skills_data = []
                
                for skill_name, skill_obj in skills.items():
                    skill_name_str = str(skill_name).split(".")[-1].lower()
                    skills_data.append({
                        "skill": skill_name_str,
                        "experience": getattr(skill_obj, "experience", 0)
                    })
                
                return skills_data
    
    return {}