"""Personal Best (PB) submissions processors, including TOB batching."""

from datetime import datetime

from .common import (
    convert_to_ms,
    convert_from_ms,
    ensure_npc_id_for_player,
    ensure_player_by_name_then_auth,
    get_player_groups_with_global,
    create_notification,
    get_player_boss_kills,
    award_points_to_player,
    select_session_and_flag,
    ensure_can_create,
    debug_print,
    GroupConfiguration,
)


# Simple in-module cache for TOB submissions (delegates to common behavior)
toa_cache = {}


def check_player_and_clean_toa_cache(player_name):
    import time

    current_time = time.time()
    expired_players = []
    for cached_player, cache_data in toa_cache.items():
        if current_time - cache_data["timestamp"] > 10:
            expired_players.append(cached_player)
    for player in expired_players:
        del toa_cache[player]
    if player_name in toa_cache:
        cache_data = toa_cache[player_name]
        if current_time - cache_data["timestamp"] <= 10:
            return cache_data["submissions"]
        else:
            del toa_cache[player_name]
    return None


def add_to_toa_cache(player_name, pb_data):
    import time

    current_time = time.time()
    if player_name not in toa_cache:
        toa_cache[player_name] = {"submissions": [], "timestamp": current_time}
    toa_cache[player_name]["submissions"].append(pb_data)
    toa_cache[player_name]["timestamp"] = current_time


def get_best_amascut_submission(submissions):
    if not submissions:
        return None
    tob_submissions = [
        sub
        for sub in submissions
        if ("Amascut" in sub.get("npc_name", "") or "Amascut" in sub.get("boss_name", ""))
        or (
            "Theatre of Blood" in sub.get("npc_name", "")
            or "Theatre of Blood" in sub.get("boss_name", "")
        )
    ]
    if not tob_submissions:
        return submissions[0]

    def get_team_size_numeric(team_size):
        if team_size == "Solo":
            return 1
        try:
            return int(team_size)
        except (ValueError, TypeError):
            return 1

    return max(tob_submissions, key=lambda x: get_team_size_numeric(x.get("team_size", 1)))


def clear_player_from_cache(player_name):
    if player_name in toa_cache:
        del toa_cache[player_name]


async def delayed_amascut_processor(player_name, external_session=None):
    import asyncio

    await asyncio.sleep(10)
    cached_submissions = check_player_and_clean_toa_cache(player_name)
    if cached_submissions:
        best_submission = get_best_amascut_submission(cached_submissions)
        if best_submission:
            debug_print(
                f"Processing delayed TOB submission for {player_name} with team size: {best_submission.get('team_size', 1)}"
            )
            clear_player_from_cache(player_name)
            await process_amascut_submission_directly(best_submission, external_session)
        else:
            clear_player_from_cache(player_name)
    else:
        debug_print(f"No cached submissions found for {player_name} after delay")


async def process_amascut_submission_directly(pb_data, external_session=None):
    debug_print(f"=== DIRECT TOB PROCESSOR START ===")
    debug_print(f"Raw PB data: {pb_data}")
    debug_print(f"External session provided: {external_session is not None}")

    session, use_external_session = select_session_and_flag(external_session)
    debug_print(f"Using external session: {use_external_session}")
    player_name = pb_data["player_name"]
    account_hash = pb_data["acc_hash"]
    boss_name = pb_data.get("npc_name", pb_data.get("boss_name", None))
    current_ms = pb_data.get("current_time_ms", pb_data.get("kill_time", 0))
    pb_ms = pb_data.get("personal_best_ms", pb_data.get("best_time", 0))
    pb_ms = convert_to_ms(pb_ms)
    current_ms = convert_to_ms(current_ms)
    if pb_ms == 0 and current_ms == 0:
        return
    team_size = pb_data.get("team_size", 1)
    is_personal_best = pb_data.get("is_new_pb", pb_data.get("is_pb", False))
    is_personal_best = True if is_personal_best == "true" else False
    time_ms = (
        current_ms if current_ms < pb_ms and current_ms != 0 else (pb_ms if pb_ms != 0 else current_ms)
    )
    auth_key = pb_data.get("auth_key", "")
    attachment_url = pb_data.get("attachment_url", None)
    attachment_type = pb_data.get("attachment_type", None)
    downloaded = pb_data.get("downloaded", False)
    image_url = pb_data.get("image_url", None)
    used_api = pb_data.get("used_api", False)
    unique_id = pb_data.get("guid", None)

    if not await ensure_can_create(session, unique_id, "pb"):
        debug_print(
            f"Personal Best entry with Unique ID {unique_id} already exists in the database, aborting"
        )
        return

    player = None
    dl_path = None
    npc_name = boss_name
    npc_id, npc_name = await ensure_npc_id_for_player(
        session, npc_name, 0, player_name, use_external_session
    )
    if npc_id is None:
        return
    player, authed, user_exists = await ensure_player_by_name_then_auth(
        session, player_name, account_hash, auth_key
    )
    if not player:
        return
    player_id = player.player_id
    if not user_exists or not authed:
        return
    from db import PersonalBestEntry

    pb_entry = (
        session.query(PersonalBestEntry)
        .filter(
            PersonalBestEntry.player_id == player_id,
            PersonalBestEntry.npc_id == npc_id,
            PersonalBestEntry.team_size == team_size,
        )
        .first()
    )
    old_time = None

    if is_personal_best:
        if attachment_url and not downloaded:
            try:
                from .common import get_extension_from_content_type, download_player_image

                file_extension = get_extension_from_content_type(attachment_type)
                file_name = f"pb_{player_id}_{boss_name.replace(' ', '_')}_{int(datetime.now().timestamp())}"
                dl_path, external_url = await download_player_image(
                    submission_type="pb",
                    file_name=file_name,
                    player=player,
                    attachment_url=attachment_url,
                    file_extension=file_extension,
                    entry_id=pb_entry.id if pb_entry else 0,
                    entry_name=boss_name,
                )
                if external_url:
                    pb_entry.image_url = external_url
                    session.commit()
            except Exception as e:
                from .common import app_logger

                app_logger.log(
                    log_type="error",
                    data=f"Couldn't download PB image: {e}",
                    app_name="core",
                    description="pb_processor",
                )
        elif downloaded:
            dl_path = image_url
    if pb_entry:
        if pb_entry.personal_best > current_ms:
            old_time = pb_entry.personal_best
            pb_entry.personal_best = time_ms
            pb_entry.new_pb = is_personal_best
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
            unique_id=unique_id,
        )
        session.add(pb_entry)
        session.commit()
        session.refresh(pb_entry)

    session.commit()
    if is_personal_best:
        try:
            current_kc = await get_player_boss_kills(player_name, npc_name)
            if current_kc >= 50:
                award_points_to_player(
                    player_id=player_id,
                    amount=20,
                    source=f"New Personal Best ({convert_from_ms(time_ms)}) at {npc_name}",
                    expires_in_days=60,
                )
        except Exception:
            pass
        player_groups = get_player_groups_with_global(session, player)
        for group in player_groups:
            group_id = group.group_id
            pb_notify_config = (
                session.query(GroupConfiguration)
                .filter(
                    GroupConfiguration.group_id == group_id,
                    GroupConfiguration.config_key == "notify_pbs",
                )
                .first()
            )
            if pb_notify_config and pb_notify_config.config_value.lower() == "true" or int(pb_notify_config.config_value) == 1:
                notification_data = {
                    "player_name": player_name,
                    "player_id": player_id,
                    "pb_id": pb_entry.id,
                    "npc_id": npc_id,
                    "boss_name": boss_name,
                    "time_ms": time_ms,
                    "old_time_ms": old_time,
                    "team_size": team_size,
                    "kill_time_ms": current_ms,
                    "image_url": pb_entry.image_url,
                }
                await create_notification(
                    "pb",
                    player_id,
                    notification_data,
                    group_id,
                    existing_session=session if use_external_session else None,
                )
                if player and player.user:
                    from .common import is_user_dm_enabled

                    if is_user_dm_enabled(session, player.user_id, "dm_pbs"):
                        await create_notification(
                            "dm_pb",
                            player_id,
                            notification_data,
                            group_id,
                            existing_session=session if use_external_session else None,
                        )
    debug_print(f"=== DIRECT TOB PROCESSOR END ===")
    return pb_entry


async def pb_processor(pb_data, external_session=None):
    debug_print(f"=== PB PROCESSOR START ===")
    debug_print(f"Raw PB data: {pb_data}")
    debug_print(f"External session provided: {external_session is not None}")

    session, use_external_session = select_session_and_flag(external_session)
    debug_print(f"Using external session: {use_external_session}")
    player_name = pb_data["player_name"]
    account_hash = pb_data["acc_hash"]
    boss_name = pb_data.get("npc_name", pb_data.get("boss_name", None))
    current_ms = pb_data.get("current_time_ms", pb_data.get("kill_time", 0))
    pb_ms = pb_data.get("personal_best_ms", pb_data.get("best_time", 0))
    pb_ms = convert_to_ms(pb_ms)
    current_ms = convert_to_ms(current_ms)
    if pb_ms == 0 and current_ms == 0:
        return
    team_size = pb_data.get("team_size", 1)
    is_personal_best = pb_data.get("is_new_pb", pb_data.get("is_pb", False))
    is_personal_best = True if is_personal_best == "true" else False
    time_ms = (
        current_ms if current_ms < pb_ms and current_ms != 0 else (pb_ms if pb_ms != 0 else current_ms)
    )
    auth_key = pb_data.get("auth_key", "")
    attachment_url = pb_data.get("attachment_url", None)
    attachment_type = pb_data.get("attachment_type", None)
    downloaded = pb_data.get("downloaded", False)
    image_url = pb_data.get("image_url", None)
    used_api = pb_data.get("used_api", False)
    unique_id = pb_data.get("guid", None)

    is_tob_submission = ("Amascut" in (boss_name or "")) or ("Theatre of Blood" in (boss_name or ""))
    if is_tob_submission:
        cached_submissions = check_player_and_clean_toa_cache(player_name)
        import asyncio

        if cached_submissions:
            add_to_toa_cache(player_name, pb_data)
            return None
        else:
            add_to_toa_cache(player_name, pb_data)
            asyncio.create_task(delayed_amascut_processor(player_name, external_session))
            return None

    if not await ensure_can_create(session, unique_id, "pb"):
        debug_print(
            f"Personal Best entry with Unique ID {unique_id} already exists in the database, aborting"
        )
        return

    player = None
    dl_path = None
    npc_name = boss_name
    npc_id, npc_name = await ensure_npc_id_for_player(
        session, npc_name, 0, player_name, use_external_session
    )
    if npc_id is None:
        return
    player, authed, user_exists = await ensure_player_by_name_then_auth(
        session, player_name, account_hash, auth_key
    )
    if not player:
        return
    player_id = player.player_id
    if not user_exists or not authed:
        return
    from db import PersonalBestEntry

    pb_entry = (
        session.query(PersonalBestEntry)
        .filter(
            PersonalBestEntry.player_id == player_id,
            PersonalBestEntry.npc_id == npc_id,
            PersonalBestEntry.team_size == team_size,
        )
        .first()
    )
    old_time = None

    if is_personal_best:
        if attachment_url and not downloaded:
            try:
                from .common import get_extension_from_content_type, download_player_image

                file_extension = get_extension_from_content_type(attachment_type)
                file_name = f"pb_{player_id}_{boss_name.replace(' ', '_')}_{int(datetime.now().timestamp())}"
                dl_path, external_url = await download_player_image(
                    submission_type="pb",
                    file_name=file_name,
                    player=player,
                    attachment_url=attachment_url,
                    file_extension=file_extension,
                    entry_id=pb_entry.id if pb_entry else 0,
                    entry_name=boss_name,
                )
                if external_url:
                    pb_entry.image_url = external_url
                    session.commit()
            except Exception as e:
                from .common import app_logger

                app_logger.log(
                    log_type="error",
                    data=f"Couldn't download PB image: {e}",
                    app_name="core",
                    description="pb_processor",
                )
        elif downloaded:
            dl_path = image_url
    if pb_entry:
        if pb_entry.personal_best > current_ms:
            old_time = pb_entry.personal_best
            pb_entry.personal_best = time_ms
            pb_entry.new_pb = is_personal_best
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
            unique_id=unique_id,
        )
        session.add(pb_entry)
        session.commit()
        session.refresh(pb_entry)

    session.commit()
    if is_personal_best:
        try:
            current_kc = await get_player_boss_kills(player_name, npc_name)
            if current_kc >= 50:
                award_points_to_player(
                    player_id=player_id,
                    amount=20,
                    source=f"New Personal Best ({convert_from_ms(time_ms)}) at {npc_name}",
                    expires_in_days=60,
                )
        except Exception:
            pass
        player_groups = get_player_groups_with_global(session, player)
        for group in player_groups:
            group_id = group.group_id
            pb_notify_config = (
                session.query(GroupConfiguration)
                .filter(
                    GroupConfiguration.group_id == group_id,
                    GroupConfiguration.config_key == "notify_pbs",
                )
                .first()
            )
            if pb_notify_config and pb_notify_config.config_value.lower() == "true" or int(pb_notify_config.config_value) == 1:
                notification_data = {
                    "player_name": player_name,
                    "player_id": player_id,
                    "pb_id": pb_entry.id,
                    "npc_id": npc_id,
                    "boss_name": boss_name,
                    "time_ms": time_ms,
                    "old_time_ms": old_time,
                    "team_size": team_size,
                    "kill_time_ms": current_ms,
                    "image_url": pb_entry.image_url,
                }
                await create_notification(
                    "pb",
                    player_id,
                    notification_data,
                    group_id,
                    existing_session=session if use_external_session else None,
                )
                if player and player.user:
                    from .common import is_user_dm_enabled

                    if is_user_dm_enabled(session, player.user_id, "dm_pbs"):
                        await create_notification(
                            "dm_pb",
                            player_id,
                            notification_data,
                            group_id,
                            existing_session=session if use_external_session else None,
                        )

    debug_print(f"=== PB PROCESSOR END ===")
    return pb_entry


