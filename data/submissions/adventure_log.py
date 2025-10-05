"""Adventure Log submissions processor."""

from .common import (
    select_session_and_flag,
    ensure_player_by_name_then_auth,
    convert_to_ms,
    get_true_boss_name,
    debug_print,
)


async def adventure_log_processor(adventure_log_data, external_session=None):
    debug_print("adventure_log_processor")
    print("Got adventure log data:", adventure_log_data)
    session, use_external_session = select_session_and_flag(external_session)

    player_name = adventure_log_data.get("player_name", adventure_log_data.get("player", None))
    account_hash = adventure_log_data.get("acc_hash", adventure_log_data.get("account_hash", None))
    player, authed, user_exists = await ensure_player_by_name_then_auth(
        session, player_name, account_hash, ""
    )
    if not player:
        return
    player_id = player.player_id
    if not user_exists or not authed:
        return

    if adventure_log_data.get("adventure_log", None):
        print("Adventure log data decoded properly...")
        adventure_log = adventure_log_data.get("adventure_log", None)
        adventure_log = adventure_log.replace("[", "").replace("]", "")
        adventure_log = adventure_log.split(",")
        if len(adventure_log) > 0:
            try:
                pb_content = adventure_log
                personal_bests = pb_content.split("\n")
                from db import PersonalBestEntry

                for pb in personal_bests:
                    boss_name, rest = pb.split(" - ")
                    team_size, time = rest.split(" : ")
                    boss_name = boss_name.strip()
                    team_size = team_size.strip()
                    boss_name, team_size, time = (
                        boss_name.replace("`", ""),
                        team_size.replace("`", ""),
                        time.replace("`", ""),
                    )
                    time = time.strip()
                    real_boss_name, npc_id = get_true_boss_name(boss_name)
                    existing_pb = (
                        session.query(PersonalBestEntry)
                        .filter(
                            PersonalBestEntry.player_id == player_id,
                            PersonalBestEntry.npc_id == npc_id,
                            PersonalBestEntry.team_size == team_size,
                        )
                        .first()
                    )
                    time_ms = convert_to_ms(time)
                    if existing_pb:
                        if time_ms < existing_pb.personal_best:
                            existing_pb.personal_best = time_ms
                            session.commit()
                    else:
                        new_pb = PersonalBestEntry(
                            player_id=player_id,
                            npc_id=npc_id,
                            team_size=team_size,
                            personal_best=time_ms,
                            kill_time=time_ms,
                            new_pb=True,
                        )
                        session.add(new_pb)
                        session.commit()
            except ValueError:
                pet_list = adventure_log_data.get("pet_list", None)
                pet_list = pet_list.replace("[", "").replace("]", "")
                pet_list = pet_list.split(",")
                if len(pet_list) > 0:
                    for pet in pet_list:
                        pet = int(pet.strip())
                        from db import ItemList, PlayerPet

                        item_object = (
                            session.query(ItemList).filter(ItemList.item_id == pet).first()
                        )
                        if item_object:
                            player_pet = PlayerPet(
                                player_id=player_id,
                                item_id=item_object.item_id,
                                pet_name=item_object.item_name,
                            )
                            try:
                                session.add(player_pet)
                                session.commit()
                                print(
                                    "Added a pet to the database for",
                                    player_name,
                                    account_hash,
                                    item_object.item_name,
                                    item_object.item_id,
                                )
                            except Exception as e:
                                print("Couldn't add a pet to the database:", e)
                                session.rollback()


