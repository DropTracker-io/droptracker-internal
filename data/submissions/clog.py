"""Collection Log submissions processor."""

from datetime import datetime

from .common import (
    ensure_item_by_name,
    ensure_player_by_name_then_auth,
    ensure_npc_id_for_player,
    get_player_groups_with_global,
    create_notification,
    is_user_dm_enabled,
    select_session_and_flag,
    ensure_can_create,
    debug_print,
    GroupConfiguration,
    award_points_to_player,
)


async def clog_processor(clog_data, external_session=None):
    debug_print(f"=== CLOG PROCESSOR START ===")
    debug_print(f"Raw clog data: {clog_data}")
    debug_print(f"External session provided: {external_session is not None}")

    debug_test = False
    player_name = clog_data.get("player_name", clog_data.get("player", None))
    if player_name == "joelhalen":
        debug_test = True
    session, use_external_session = select_session_and_flag(external_session)
    debug_print(f"Using external session: {use_external_session}")
    if not player_name:
        debug_print("No player name found, aborting")
        return
    has_xf_entry = False

    account_hash = clog_data["acc_hash"]
    item_name = clog_data.get("item_name", clog_data.get("item", None))
    if not item_name:
        debug_print("No item name found, aborting")
        return
    auth_key = clog_data.get("auth_key", "")
    attachment_url = clog_data.get("attachment_url", None)
    attachment_type = clog_data.get("attachment_type", None)
    reported_slots = clog_data.get("reported_slots", None)
    downloaded = clog_data.get("downloaded", False)
    image_url = clog_data.get("image_url", None)
    used_api = clog_data.get("used_api", False)
    killcount = clog_data.get("kc", None)
    unique_id = clog_data.get("guid", None)
    item = await ensure_item_by_name(session, item_name)

    if not await ensure_can_create(session, unique_id, "clog"):
        print(
            f"Collection Log entry with Unique ID {unique_id} already exists in the database, aborting"
        )
        return
    if not item:
        print(f"Item {item_name} not found in database, aborting")
        return
    item_id = item.item_id
    npc_name = clog_data.get("source", None)
    npc = npc_name
    print(f"NPC: {npc}")
    npc_id = None
    if player_name is None:
        return
    player, authed, user_exists = await ensure_player_by_name_then_auth(
        session, player_name, account_hash, auth_key
    )
    if not player:
        print(f"Player does not exist, and creating failed")
        return
    player_id = player.player_id
    npc_id, npc_name = await ensure_npc_id_for_player(
        session, npc_name, player_id, player_name, use_external_session
    )
    if npc_id is None:
        return
    player = session.query(type(player)).filter(type(player).player_id == player_id).first()
    if not player:
        print("Player not found in database, aborting")
        return
    if not user_exists or not authed:
        print("user failed auth check")
        return

    from db import CollectionLogEntry, User, UserConfiguration

    clog_entry = (
        session.query(CollectionLogEntry)
        .filter(
            CollectionLogEntry.player_id == player_id,
            CollectionLogEntry.item_id == item_id,
        )
        .first()
    )

    is_new_clog = False
    if npc_id is None:
        print(f"We did not find an npc for {npc_name}, aborting")
        return
    if not clog_entry:
        clog_entry = CollectionLogEntry(
            player_id=player_id,
            reported_slots=reported_slots,
            item_id=item_id,
            npc_id=npc_id,
            date_added=datetime.now(),
            image_url="",
            used_api=used_api,
            unique_id=unique_id,
        )
        session.add(clog_entry)
        session.commit()

        if attachment_url and not downloaded:
            try:
                from .common import get_extension_from_content_type, download_player_image

                file_extension = get_extension_from_content_type(attachment_type)
                file_name = f"clog_{player_id}_{item_name.replace(' ', '_')}_{int(datetime.now().timestamp())}"
                dl_path, external_url = await download_player_image(
                    submission_type="clog",
                    file_name=file_name,
                    player=player,
                    attachment_url=attachment_url,
                    file_extension=file_extension,
                    entry_id=clog_entry.log_id,
                    entry_name=item_name,
                )
                clog_entry.image_url = external_url if external_url else ""
            except Exception as e:
                from .common import app_logger

                app_logger.log(
                    log_type="error",
                    data=f"Couldn't download collection log image: {e}",
                    app_name="core",
                    description="clog_processor",
                )
        elif downloaded:
            clog_entry.image_url = image_url

        is_new_clog = True
        print("Added clog to session")
    print("Committing session")
    session.commit()

    if is_new_clog:
        print("New collection log -- Creating notification")
        award_points_to_player(
            player_id=player_id,
            amount=5,
            source=f"Collection Log slot: {item_name}",
            expires_in_days=60,
        )
        player_groups = get_player_groups_with_global(session, player)
        for group in player_groups:
            print(f"CLOG: Checking group: {group}")
            group_id = group.group_id
            clog_notify_config = (
                session.query(GroupConfiguration)
                .filter(
                    GroupConfiguration.group_id == group_id,
                    GroupConfiguration.config_key == "notify_clogs",
                )
                .first()
            )
            if clog_notify_config and (
                clog_notify_config.config_value.lower() == "true"
                or int(clog_notify_config.config_value) == 1
            ):
                notification_data = {
                    "player_name": player_name,
                    "player_id": player_id,
                    "item_name": item_name,
                    "npc_name": npc,
                    "image_url": clog_entry.image_url,
                    "kc_received": killcount,
                    "item_id": item_id,
                }
                await create_notification(
                    "clog",
                    player_id,
                    notification_data,
                    group_id,
                    existing_session=session if use_external_session else None,
                )
        if player and player.user:
            user = session.query(User).filter(User.user_id == player.user_id).first()
            if user and is_user_dm_enabled(session, user.user_id, "dm_clogs"):
                await create_notification(
                    "dm_clog",
                    player_id,
                    {
                        "player_name": player_name,
                        "player_id": player_id,
                        "item_name": item_name,
                        "npc_name": npc,
                        "image_url": clog_entry.image_url,
                        "kc_received": killcount,
                        "item_id": item_id,
                    },
                    group_id,
                    existing_session=session if use_external_session else None,
                )
    debug_print("Returning clog entry")
    debug_print(f"=== CLOG PROCESSOR END ===")
    return clog_entry


