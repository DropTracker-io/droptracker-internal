"""Drop submissions processor."""

from datetime import datetime, timedelta
import time

from .common import (
    SubmissionResponse,
    ensure_item_for_drop,
    ensure_player_and_auth,
    ensure_npc_id_for_player,
    resolve_attachment_from_drop_data,
    get_player_groups_with_global,
    is_user_dm_enabled,
    select_session_and_flag,
    create_notification,
    get_point_divisor,
    get_true_item_value,
    RedisClient,
    DatabaseOperations,
    debug_print,
    GroupConfiguration,
    FeatureActivation,
    award_points_to_player,
    player_list,
    redis_updates,
)


redis_client = RedisClient()
db = DatabaseOperations()
last_board_updates = {}


async def drop_processor(drop_data, external_session=None):
    """Process a drop submission and create notifications when appropriate."""

    debug_print(f"=== DROP PROCESSOR START ===")
    debug_print(f"Raw drop data: {drop_data}")
    debug_print(f"External session provided: {external_session is not None}")

    session, use_external_session = select_session_and_flag(external_session)
    debug_print(f"Using external session: {use_external_session}")

    try:
        npc_name = drop_data.get("source", drop_data.get("npc_name", None))
        value = drop_data["value"]
        item_id = drop_data.get("item_id", drop_data.get("id", None))
        item_name = drop_data.get("item_name", drop_data.get("item", None))
        quantity = drop_data["quantity"]
        auth_key = drop_data.get("auth_key", None)
        player_name = drop_data.get("player_name", drop_data.get("player", None))
        account_hash = drop_data["acc_hash"]
        kill_count = drop_data.get("kill_count", None)
        player_name = str(player_name).strip()
        account_hash = str(account_hash)
        guid = drop_data.get("guid", None)
        downloaded = drop_data.get("downloaded", False)
        image_url = drop_data.get("image_url", None)
        used_api = drop_data.get("used_api", False)

        # dedupe via NotifiedSubmission cache in caller; keep local prevention via ensure_can_create
        from .common import ensure_can_create

        if not await ensure_can_create(session, guid, "drop"):
            return

        debug_print(
            f"Extracted data - Player: {player_name}, Item: {item_name} (ID: {item_id}), NPC: {npc_name}"
        )
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
        player, authed, user_exists = await ensure_player_and_auth(
            session, player_name, account_hash, auth_key
        )
        if not player:
            debug_print("Player not found in the database")
            return SubmissionResponse(success=False, message=f"Player {player_name} not found in the database")
        if not user_exists or not authed:
            debug_print(player_name + " failed auth check")
            return SubmissionResponse(success=False, message=f"Player {player_name} failed auth check")
        debug_print(
            f"Player validated - ID: {player.player_id}, Name: {player_name}, Authed: {authed}"
        )

        debug_print(f"Ensuring NPC ID for {npc_name}...")
        npc_id, npc_name = await ensure_npc_id_for_player(
            session, npc_name, player.player_id, player_name, use_external_session
        )
        if npc_id is None:
            debug_print(f"NPC ID could not be resolved for {npc_name}, aborting")
            return
        debug_print(f"NPC validated - ID: {npc_id}, Name: {npc_name}")

        player_id = player_list[player_name]
        item_cache = redis_client.get(item_id)
        if not item_cache:
            item_cache = session.query(type(item).item_id).filter(type(item).item_id == item_id).first()
        if item_cache:
            redis_client.set(item_id, item_id)
        else:
            notification_data = {
                "item_name": item_name,
                "player_name": player_name,
                "item_id": item_id,
                "npc_name": npc_name,
                "value": value,
            }
            await create_notification(
                "new_item",
                player_id,
                notification_data,
                existing_session=session if use_external_session else None,
            )
            debug_print(f"Item not found... {item_id} {item_name}")
            return SubmissionResponse(
                success=False, message=f"Item {item_name} not found in the database"
            )

        debug_print(f"Calculating drop value...")
        raw_drop_value = await get_true_item_value(item_name, int(value))
        drop_value = int(raw_drop_value) * int(quantity)
        debug_print(
            f"Drop value calculated - Raw: {raw_drop_value}, Total: {drop_value} ({quantity}x)"
        )

        if drop_value > 1000000:
            debug_print(f"High value drop detected, verifying item/NPC combination...")
            from .common import osrs_api

            async with osrs_api.create_client() as client:
                is_from_npc = await client.semantic.check_drop(item_name, npc_name)
            if not is_from_npc:
                debug_print(f"Verification failed: {item_name} is not from {npc_name}")
                return SubmissionResponse(
                    success=False, message=f"Item {item_name} is not from NPC {npc_name}"
                )
            debug_print(f"Item/NPC combination verified successfully")

        debug_print(f"Processing attachment data...")
        attachment_url, attachment_type = resolve_attachment_from_drop_data(drop_data)
        debug_print(f"Attachment resolved - URL: {attachment_url}, Type: {attachment_type}")

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
            used_api=used_api,
            unique_id=guid,
            existing_session=session if use_external_session else None,
        )
        debug_print(f"Drop created successfully - Drop ID: {drop.drop_id if drop else 'None'}")
        if not drop:
            debug_print("Failed to create drop")
            return SubmissionResponse(success=False, message=f"Failed to create drop")
        try:
            debug_print("Updating player in redis...")
            redis_updates.add_to_player(player, drop)
            debug_print("Player redis update completed")
        except Exception as e:
            debug_print(f"Error updating player in redis: {e}")
            session.rollback()
            return

        debug_print(f"Getting player groups for {player_name}...")
        player_groups = get_player_groups_with_global(session, player)
        debug_print(
            f"Player groups found: {[group.group_name for group in player_groups]}"
        )
        sent_group_notifications = []
        debug_print(f"Processing notifications for {len(player_groups)} groups...")
        has_awarded_points = False
        for group in player_groups:
            group_id = group.group_id
            debug_print(f"Processing group: {group.group_name} (ID: {group_id})")

            min_value_config = (
                session.query(GroupConfiguration)
                .filter(
                    GroupConfiguration.group_id == group_id,
                    GroupConfiguration.config_key == "minimum_value_to_notify",
                )
                .first()
            )
            min_value_to_notify = int(min_value_config.config_value) if min_value_config else 2500000
            debug_print(f"Group {group_id} minimum value to notify: {min_value_to_notify}")

            should_send_stacks = (
                session.query(GroupConfiguration)
                .filter(
                    GroupConfiguration.group_id == group_id,
                    GroupConfiguration.config_key == "send_stacks_of_items",
                )
                .first()
            )
            send_stacks = False
            if should_send_stacks:
                if should_send_stacks.config_value == "1" or should_send_stacks.config_value == "true":
                    send_stacks = True

            debug_print(
                f"Checking notification criteria - Raw value: {raw_drop_value}, Drop value: {drop_value}, Send stacks: {send_stacks}"
            )
            player_dm_sent = False
            if int(raw_drop_value) >= min_value_to_notify or (
                send_stacks == True and int(drop_value) > min_value_to_notify
            ):
                debug_print(f"Notification criteria met for group {group_id}")
                point_divisor = get_point_divisor()
                if group_id != 2 and has_awarded_points == False and int(drop_value) > point_divisor:
                    print(
                        f"Awarding points to {player_name} for drop {item_name} from {npc_name}"
                    )
                    has_awarded_points = True
                    points_to_award = int(drop_value / point_divisor)
                    award_points_to_player(
                        player_id=player_id,
                        amount=points_to_award,
                        source=f"Drop: {item_name} from {npc_name}",
                        expires_in_days=60,
                    )
                notification_data = {
                    "drop_id": drop.drop_id,
                    "item_name": item_name,
                    "npc_name": npc_name,
                    "value": value,
                    "quantity": quantity,
                    "total_value": drop_value,
                    "kill_count": kill_count,
                    "player_name": player_name,
                    "player_id": player_id,
                    "image_url": drop.image_url,
                    "attachment_type": attachment_type,
                }
                if group_id > 2:
                    sent_group_notifications.append(group.group_name)
                    debug_print(f"Added {group.group_name} to notification list")

                if player and player_dm_sent == False:
                    if player.user:
                        if is_user_dm_enabled(session, player.user_id, "dm_drops"):
                            debug_print(f"Creating DM notification for user {player.user_id}")
                            await create_notification(
                                "dm_drop",
                                player_id,
                                notification_data,
                                group_id,
                                existing_session=session if use_external_session else None,
                            )
                            player_dm_sent = True
                debug_print(f"Creating group notification for {player_name} in group {group_id}")
                await create_notification(
                    "drop",
                    player_id,
                    notification_data,
                    group_id,
                    existing_session=session if use_external_session else None,
                )
                should_instantly_update = (
                    session.query(FeatureActivation)
                    .filter(
                        FeatureActivation.group_id == group_id,
                        FeatureActivation.feature_id == 2,
                        FeatureActivation.status == "active",
                    )
                    .first()
                )
                if group_id == 2 or should_instantly_update:
                    if group_id not in last_board_updates:
                        last_board_updates[group_id] = datetime.now() - timedelta(seconds=10)
                    if last_board_updates[group_id] > datetime.now() - timedelta(seconds=10):
                        debug_print(
                            f"Skipping group {group_id}: within 10 second window for instant update"
                        )
                        continue
                    last_board_updates[group_id] = datetime.now()
            else:
                debug_print(
                    f"Notification criteria NOT met for group {group_id} - skipping"
                )
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
                group_name = {", ".join(sent_group_notifications)}
            debug_print(
                f"Returning success with group notifications: {group_name}"
            )
            debug_print(f"=== DROP PROCESSOR END (SUCCESS) ===")
            return SubmissionResponse(
                success=True,
                message=f"Drop created successfully",
                notice=f"Drop processed - a message has been sent to {group_name} for you",
            )
        else:
            debug_print(f"Returning success without group notifications")
            debug_print(f"=== DROP PROCESSOR END (SUCCESS) ===")
            return SubmissionResponse(success=True, message=f"Drop created successfully")

    except Exception as e:
        if not use_external_session:
            debug_print(f"Exception occurred, rolling back session: {e}")
            session.rollback()
        else:
            debug_print(f"Exception occurred with external session: {e}")
        debug_print(f"Error in drop_processor: {e}")
        debug_print(f"=== DROP PROCESSOR END (ERROR) ===")
        raise


