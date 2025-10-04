from urllib.parse import quote

from utils.format import convert_from_ms, format_number
from api.core import get_db_session
from db import Player, PersonalBestEntry, NpcList, Drop, ItemList, CollectionLogEntry


async def assemble_submission_data(submissions, db_session=None):
    if not db_session:
        db_session = get_db_session()
    final_submission_data = []
    drops = []
    pbs = []
    clogs = []
    try:
        for submission in submissions:
            submission_data = {}
            submission_data["date_received"] = submission.date_added.isoformat()
            player = db_session.query(Player).filter(Player.player_id == submission.player_id).first()
            submission_data["player_name"] = player.player_name
            if submission.pb_id:
                sub_type = "pb"
            elif submission.drop_id:
                sub_type = "drop"
            elif submission.clog_id:
                sub_type = "clog"
            else:
                continue
            submission_data["submission_type"] = sub_type
            match (sub_type):
                case "pb":
                    if submission.pb_id not in pbs:
                        pbs.append(submission.pb_id)
                    else:
                        continue
                    pb_entry: PersonalBestEntry = db_session.query(PersonalBestEntry).filter(PersonalBestEntry.id == submission.pb_id).first()
                    raw_best_time = pb_entry.personal_best if (pb_entry.personal_best > 0 and pb_entry.personal_best < pb_entry.kill_time) else pb_entry.kill_time if pb_entry.kill_time > 0 else 0
                    if raw_best_time == 0:
                        continue
                    npc = db_session.query(NpcList).filter(NpcList.npc_id == pb_entry.npc_id).first()
                    submission_data["display_name"] = "Personal Best: " + convert_from_ms(raw_best_time)
                    submission_data["image_url"] = f"https://www.droptracker.io/img/npcdb/{pb_entry.npc_id}.png"
                    submission_data["source_name"] = npc.npc_name
                    if pb_entry.image_url:
                        submission_img_url = str(pb_entry.image_url)
                        submission_data["submission_image_url"] = submission_img_url.replace("/store/droptracker/disc/static/assets/img/", "https://www.droptracker.io/img/")
                        submission_data["submission_image_url"] = quote(submission_data["submission_image_url"], safe=':/?#[]@!$&\'()*+,;=')
                    submission_data["data"] = [
                        {
                            "type": "best_time",
                            "time": convert_from_ms(raw_best_time),
                            "team_size": pb_entry.team_size,
                        }
                    ]
                case "drop":
                    if submission.drop_id not in drops:
                        drops.append(submission.drop_id)
                    else:
                        continue
                    drop_entry: Drop = db_session.query(Drop).filter(Drop.drop_id == submission.drop_id).first()
                    item = db_session.query(ItemList).filter(ItemList.item_id == drop_entry.item_id).first()
                    submission_data["image_url"] = f"https://www.droptracker.io/img/itemdb/{item.item_id}.png"
                    npc = db_session.query(NpcList).filter(NpcList.npc_id == drop_entry.npc_id).first()
                    submission_data["source_name"] = npc.npc_name
                    submission_data["display_name"] = f"{item.item_name}"
                    drop_value = drop_entry.value * drop_entry.quantity
                    submission_data["value"] = format_number(drop_value)
                    if drop_entry.image_url:
                        submission_img_url = str(drop_entry.image_url)
                        submission_data["submission_image_url"] = submission_img_url.replace("/store/droptracker/disc/static/assets/img/", "https://www.droptracker.io/img/")
                        submission_data["submission_image_url"] = quote(submission_data["submission_image_url"], safe=':/?#[]@!$&\'()*+,;=')
                    submission_data["data"] = [
                        {
                            "type": "item",
                            "value": format_number(drop_value),
                            "name": item.item_name,
                            "id": item.item_id,
                        }
                    ]
                case "clog":
                    if submission.clog_id not in clogs:
                        clogs.append(submission.clog_id)
                    else:
                        continue
                    clog_entry: CollectionLogEntry = db_session.query(CollectionLogEntry).filter(CollectionLogEntry.log_id == submission.clog_id).first()
                    item = db_session.query(ItemList).filter(ItemList.item_id == clog_entry.item_id).first()
                    submission_data["image_url"] = f"https://www.droptracker.io/img/itemdb/{item.item_id}.png"
                    submission_data["source_name"] = item.item_name
                    submission_data["display_name"] = f"{item.item_name}"
                    if clog_entry.image_url:
                        submission_img_url = str(clog_entry.image_url)
                        submission_data["submission_image_url"] = submission_img_url.replace("/store/droptracker/disc/static/assets/img/", "https://www.droptracker.io/img/")
                        submission_data["submission_image_url"] = quote(submission_data["submission_image_url"], safe=':/?#[]@!$&\'()*+,;=')
                    submission_data["data"] = [
                        {
                            "type": "clog_item",
                            "name": item.item_name,
                            "id": item.item_id,
                        }
                    ]
            final_submission_data.append(submission_data)
    except Exception as e:
        print(f"Error assembling submission data: {e}")
        return []
    finally:
        if db_session:
            db_session.close()
    return final_submission_data


__all__ = ["assemble_submission_data"]


