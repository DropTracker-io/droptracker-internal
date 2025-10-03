from sqlalchemy import text
from db.models import Drop, CollectionLogEntry, XenforoSession, PersonalBestEntry, CombatAchievementEntry, Group, Player, User
from utils.format import convert_from_ms
async def create_xenforo_entry(drop: Drop = None, clog: CollectionLogEntry = None, personal_best: PersonalBestEntry = None, combat_achievement: CombatAchievementEntry = None):
    with XenforoSession() as xenforo_session:
        try:
        
            if drop:
                if (drop.value * drop.quantity) > 5000000:
                    raw_sql = """
                        INSERT INTO dt_recent_submissions (type, item_id, npc_id, player_id, total_value, date)
                        VALUES (:type, :item_id, :npc_id, :player_id, :total_value, :date)
                    """
                    params = {
                        "type": "drop",
                        "item_id": drop.item_id,
                        "npc_id": drop.npc_id,
                        "player_id": drop.player_id,
                        "total_value": drop.value * drop.quantity,
                        "date": drop.date_added.timestamp()
                    }
                    xenforo_session.execute(text(raw_sql), params)
                else:
                    return True
            elif clog:
                raw_sql = """
                    INSERT INTO dt_recent_submissions (type, item_id, npc_id, player_id, date)
                    VALUES (:type, :item_id, :npc_id, :player_id, :date)
                """
                params = {
                    "type": "clog",
                    "item_id": clog.item_id,
                    "npc_id": clog.npc_id,
                    "player_id": clog.player_id,
                    "date": clog.date_added.timestamp()
                }
                xenforo_session.execute(text(raw_sql), params)
            elif personal_best:
                raw_sql = """
                    INSERT INTO dt_recent_submissions (type, npc_id, player_id, time, date)
                    VALUES (:type, :npc_id, :player_id, :time, :date)
                """
                kill_time = convert_from_ms(personal_best.personal_best) if personal_best.personal_best > 0 else convert_from_ms(personal_best.kill_time)
                params = {
                    "type": "personal_best", 
                    "npc_id": personal_best.npc_id, 
                    "player_id": personal_best.player_id, 
                    "time": kill_time,
                    "date": personal_best.date_added.timestamp()
                }
                xenforo_session.execute(text(raw_sql), params)
            elif combat_achievement:
                raw_sql = """
                    INSERT INTO dt_recent_submissions (type, achievement_name, player_id, date)
                    VALUES (:type, :achievement_name, :player_id, :date)
                """
                params = {
                    "type": "combat_achievement",
                    "achievement_name": combat_achievement.task_name,
                    "player_id": combat_achievement.player_id,
                    "date": combat_achievement.date_added.timestamp()
                }
                xenforo_session.execute(text(raw_sql), params)  
            xenforo_session.commit()
            return True
        except Exception as e:
            print("Couldn't add the submission to XenForo: (data: ", drop, clog, personal_best, combat_achievement, ")", e)
            xenforo_session.rollback()
            return False
        finally:
            xenforo_session.close()
        return True