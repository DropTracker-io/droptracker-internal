"""Combat Achievements submissions processor."""

from datetime import datetime

from .common import (
    ensure_player_by_name_then_auth,
    ensure_can_create,
    select_session_and_flag,
    create_notification,
    get_player_groups_with_global,
    award_points_to_player,
    debug_print,
    GroupConfiguration,
)


async def ca_processor(ca_data, external_session=None):
    debug_print(f"=== CA PROCESSOR START ===")
    debug_print(f"Raw CA data: {ca_data}")
    debug_print(f"External session provided: {external_session is not None}")

    has_xf_entry = False
    session, use_external_session = select_session_and_flag(external_session)
    debug_print(f"Using external session: {use_external_session}")
    player_name = ca_data["player_name"]
    account_hash = ca_data["acc_hash"]
    points_awarded = ca_data["points"]
    points_total = ca_data["total_points"]
    completed_tier = ca_data.get("completed", None)
    task_name = ca_data.get("task", None)
    tier = ca_data["tier"]
    auth_key = ca_data.get("auth_key", "")
    attachment_url = ca_data.get("attachment_url", None)
    attachment_type = ca_data.get("attachment_type", None)
    downloaded = ca_data.get("downloaded", False)
    image_url = ca_data.get("image_url", None)
    used_api = ca_data.get("used_api", False)
    unique_id = ca_data.get("guid", None)
    if player_name == "Scributles":
        print(f"CA data for Scributles: {ca_data}")
    debug_print(
        f"Extracted CA data - Player: {player_name}, Task: {task_name}, Tier: {tier}"
    )
    debug_print(
        f"Points awarded: {points_awarded}, Total points: {points_total}, Completed tier: {completed_tier}"
    )
    debug_print(
        f"Account hash: {account_hash[:8]}... (truncated), Used API: {used_api}"
    )

    player, authed, user_exists = await ensure_player_by_name_then_auth(
        session, player_name, account_hash, auth_key
    )
    if not player:
        debug_print("Player still not found in the database, aborting")
        return
    player_id = player.player_id
    if not user_exists or not authed:
        debug_print("User failed auth check")
        return

    if not await ensure_can_create(session, unique_id, "ca"):
        debug_print(
            f"Combat Achievement entry with Unique ID {unique_id} already exists in the database, aborting"
        )
        return
    from db import CombatAchievementEntry

    ca_entry = (
        session.query(CombatAchievementEntry)
        .filter(
            CombatAchievementEntry.player_id == player_id,
            CombatAchievementEntry.task_name == task_name,
        )
        .first()
    )
    is_new_ca = False

    if not ca_entry:
        debug_print(
            "CA entry not found in the database, creating new entry - Task tier: "
            + str(tier)
        )
        dl_path = ""
        ca_entry = CombatAchievementEntry(
            player_id=player_id,
            task_name=task_name,
            date_added=datetime.now(),
            image_url=dl_path,
            used_api=used_api,
            unique_id=unique_id,
        )
        session.add(ca_entry)
        is_new_ca = True
        if attachment_url and not downloaded:
            try:
                from .common import get_extension_from_content_type, download_player_image

                file_extension = get_extension_from_content_type(attachment_type)
                file_name = f"ca_{player_id}_{task_name.replace(' ', '_')}_{int(datetime.now().timestamp())}"
                player = session.query(type(player)).filter(type(player).player_id == player_id).first()
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
                    entry_name=task_name,
                )
                if external_url:
                    ca_entry.image_url = external_url
            except Exception as e:
                from .common import app_logger

                app_logger.log(
                    log_type="error",
                    data=f"Couldn't download CA image: {e}",
                    app_name="core",
                    description="ca_processor",
                )
        elif downloaded:
            ca_entry.image_url = image_url
    session.commit()
    debug_print("Committed a new CA entry")
    match str(tier).strip().lower():
        case "easy":
            points = 1
        case "medium":
            points = 2
        case "hard":
            points = 3
        case "elite":
            points = 4
        case "master":
            points = 5
        case "grandmaster":
            points = 6
        case _:
            points = 1
    try:
        award_points_to_player(
            player_id=player_id,
            amount=points,
            source=f"Combat Achievement: {task_name}",
            expires_in_days=60,
        )
    except Exception as e:
        debug_print(f"Couldn't award points to player: {e}")
        from .common import app_logger

        app_logger.log(
            log_type="error",
            data=f"Couldn't award points to player: {e}",
            app_name="core",
            description="ca_processor",
        )
    if is_new_ca:
        debug_print("New CA entry, creating notification")
        player_groups = get_player_groups_with_global(session, player)
        for group in player_groups:
            debug_print("Checking group: " + str(group))
            group_id = group.group_id
            ca_notify_config = (
                session.query(GroupConfiguration)
                .filter(
                    GroupConfiguration.group_id == group_id,
                    GroupConfiguration.config_key == "notify_cas",
                )
                .first()
            )
            debug_print("CA notify config: " + str(ca_notify_config.config_value))
            if ca_notify_config and ca_notify_config.config_value.lower() == "true" or ca_notify_config.config_value == "1":
                min_tier = (
                    session.query(GroupConfiguration.config_value)
                    .filter(
                        GroupConfiguration.config_key == "min_ca_tier_to_notify",
                        GroupConfiguration.group_id == group_id,
                    )
                    .first()
                )
                tier_order = ["easy", "medium", "hard", "elite", "master", "grandmaster"]
                if min_tier != "disabled" or group_id == 2:
                    if (min_tier and min_tier[0].lower() in tier_order) or group_id == 2:
                        min_tier_value = min_tier[0].lower()
                        min_tier_index = tier_order.index(min_tier_value)
                        task_tier_index = tier_order.index(tier.lower()) if tier.lower() in tier_order else -1
                        if task_tier_index < min_tier_index:
                            debug_print(
                                f"Skipping {task_name} ({tier}) as it's below minimum tier {min_tier_value} for group {group_id}"
                            )
                            continue
                        else:
                            debug_print("Tier meets minimum notification tier")
                            notification_data = {
                                "player_name": player_name,
                                "player_id": player_id,
                                "task_name": task_name,
                                "tier": tier,
                                "points_awarded": points_awarded,
                                "points_total": points_total,
                                "completed_tier": completed_tier,
                                "image_url": ca_entry.image_url,
                            }
                            if player and player.user:
                                user = session.query(type(player.user)).filter(type(player.user).user_id == player.user_id).first()
                                if user:
                                    from .common import is_user_dm_enabled

                                    if is_user_dm_enabled(session, user.user_id, "dm_cas"):
                                        await create_notification(
                                            "dm_ca",
                                            player_id,
                                            notification_data,
                                            group_id,
                                            existing_session=session if use_external_session else None,
                                        )
                            await create_notification(
                                "ca",
                                player_id,
                                notification_data,
                                group_id,
                                existing_session=session if use_external_session else None,
                            )
    debug_print(f"=== CA PROCESSOR END ===")
    return ca_entry


