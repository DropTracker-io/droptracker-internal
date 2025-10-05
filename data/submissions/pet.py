"""Pet submissions processor."""

from datetime import datetime

from .common import (
    ensure_player_by_name_then_auth,
    ensure_item_by_name,
    ensure_npc_id_for_player,
    get_player_groups_with_global,
    is_user_dm_enabled,
    create_notification,
    is_truthy_config,
    select_session_and_flag,
    ensure_can_create,
    debug_print,
    GroupConfiguration,
    award_points_to_player,
)


async def pet_processor(pet_data, external_session=None):
    debug_print(f"=== PET PROCESSOR START ===")
    debug_print(f"Raw pet data: {pet_data}")
    debug_print(f"External session provided: {external_session is not None}")

    session, use_external_session = select_session_and_flag(external_session)
    debug_print(f"Using external session: {use_external_session}")

    player_name = pet_data.get("player_name", pet_data.get("player", None))
    if not player_name:
        debug_print("No player name found, aborting")
        return
    account_hash = pet_data.get("acc_hash", pet_data.get("account_hash", None))
    if not account_hash:
        debug_print("No account hash found, aborting")
        return
    pet_name = pet_data.get("pet_name", None)
    if not pet_name:
        debug_print("No pet name found, aborting")
        return

    auth_key = pet_data.get("auth_key", "")
    attachment_url = pet_data.get("attachment_url", None)
    attachment_type = pet_data.get("attachment_type", None)
    downloaded = pet_data.get("downloaded", False)
    image_url = pet_data.get("image_url", None)
    used_api = pet_data.get("used_api", False)
    source = pet_data.get("source", None)
    killcount = pet_data.get("killcount", None)
    milestone = pet_data.get("milestone", None)
    duplicate = pet_data.get("duplicate", False)
    previously_owned = pet_data.get("previously_owned", None)
    game_message = pet_data.get("game_message", None)
    unique_id = pet_data.get("guid", None)
    if not await ensure_can_create(session, unique_id, "pet"):
        print(
            f"Pet entry with Unique ID {unique_id} already exists in the database, aborting"
        )
        return
    debug_print(
        f"Extracted pet data - Player: {player_name}, Pet: {pet_name}, Source: {source}"
    )
    debug_print(f"Account hash: {account_hash[:8]}... (truncated), Duplicate: {duplicate}")
    debug_print(f"Attachment URL: {attachment_url}, Type: {attachment_type}, Downloaded: {downloaded}")

    player, authed, user_exists = await ensure_player_by_name_then_auth(
        session, player_name, account_hash, auth_key
    )
    if not player:
        debug_print("Player not found in the database, aborting")
        return
    player_id = player.player_id
    if not user_exists or not authed:
        debug_print("User failed auth check")
        return

    pet_item = await ensure_item_by_name(session, pet_name)
    if not pet_item:
        debug_print(f"Pet item {pet_name} not found in database")
        pet_item_id = None
    else:
        pet_item_id = pet_item.item_id
        debug_print(f"Pet item validated - ID: {pet_item_id}, Name: {pet_name}")

    npc_id = None
    npc_name = source
    if source:
        npc_id, npc_name = await ensure_npc_id_for_player(
            session, source, player_id, player_name, use_external_session
        )
        debug_print(f"NPC resolved - ID: {npc_id}, Name: {npc_name}")

    from db import PlayerPet, User

    existing_pet = None
    new_pet = None
    if pet_item_id:
        existing_pet = (
            session.query(PlayerPet)
            .filter(PlayerPet.player_id == player_id, PlayerPet.item_id == pet_item_id)
            .first()
        )

    is_new_pet = existing_pet is None
    dl_path = ""
    if is_new_pet and pet_item_id:
        debug_print(f"Creating new pet entry for {player_name}: {pet_name}")
        try:
            new_pet = PlayerPet(player_id=player_id, item_id=pet_item_id, pet_name=pet_name)
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

    if is_new_pet and attachment_url and not downloaded:
        try:
            from .common import get_extension_from_content_type, download_player_image

            file_extension = get_extension_from_content_type(attachment_type)
            file_name = f"pet_{player_id}_{pet_name.replace(' ', '_')}_{int(datetime.now().timestamp())}"
            dl_path, external_url = await download_player_image(
                submission_type="pet",
                file_name=file_name,
                player=player,
                attachment_url=attachment_url,
                file_extension=file_extension,
                entry_id=existing_pet.id if existing_pet else 0,
                entry_name=pet_name,
            )
            if external_url:
                dl_path = external_url
        except Exception as e:
            from .common import app_logger

            app_logger.log(
                log_type="error",
                data=f"Couldn't download pet image: {e}",
                app_name="core",
                description="pet_processor",
            )
    elif downloaded:
        dl_path = image_url
    if is_new_pet:
        award_points_to_player(
            player_id=player_id, amount=50, source=f"Pet: {pet_name}", expires_in_days=60
        )

    should_notify = is_new_pet or (duplicate and not is_new_pet)
    if should_notify:
        debug_print(f"Creating notifications for pet submission")
        player_groups = get_player_groups_with_global(session, player)
        for group in player_groups:
            debug_print(f"Checking group: {group.group_name}")
            group_id = group.group_id
            pet_notify_config = (
                session.query(GroupConfiguration)
                .filter(
                    GroupConfiguration.group_id == group_id,
                    GroupConfiguration.config_key == "notify_pets",
                )
                .first()
            )
            debug_print(
                f"Pet notify config for group {group_id}: {pet_notify_config.config_value if pet_notify_config else 'None'}"
            )
            if pet_notify_config and is_truthy_config(pet_notify_config.config_value):
                debug_print(f"Group {group_id} has pet notifications enabled")
                notification_data = {
                    "group_id": group_id,
                    "player_name": player_name,
                    "player_id": player_id,
                    "pet_name": pet_name,
                    "source": source,
                    "npc_name": npc_name,
                    "killcount": killcount,
                    "milestone": milestone,
                    "duplicate": duplicate,
                    "previously_owned": previously_owned,
                    "game_message": game_message,
                    "image_url": dl_path,
                    "item_id": pet_item_id,
                    "npc_id": npc_id,
                    "is_new_pet": is_new_pet,
                }
                if player and player.user:
                    user = session.query(User).filter(User.user_id == player.user_id).first()
                    if user and is_user_dm_enabled(session, user.user_id, "dm_pets"):
                        debug_print(f"Creating DM notification for user {user.user_id}")
                        await create_notification(
                            "dm_pet",
                            player_id,
                            notification_data,
                            group_id,
                            existing_session=session if use_external_session else None,
                        )
                await create_notification(
                    "pet",
                    player_id,
                    notification_data,
                    group_id,
                    existing_session=session if use_external_session else None,
                )
                debug_print(f"Created pet notification for group {group_id}")

    debug_print(f"=== PET PROCESSOR END ===")
    return existing_pet if existing_pet else (new_pet if is_new_pet and pet_item_id else None)


