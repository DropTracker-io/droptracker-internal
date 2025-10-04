import asyncio
import json
from datetime import datetime, timedelta

from quart import Blueprint, jsonify, request
from sqlalchemy import text

from api.core import logger, metrics
from api.core import get_db_session
from db import Drop, CollectionLogEntry, PersonalBestEntry, CombatAchievementEntry, Player


utils_bp = Blueprint("utils", __name__)


@utils_bp.get("/debug_logs")
async def debug_logger():
    file = "data/logs/app_logs.json"

    def _lenient_parse_json(content: str):
        try:
            parsed = json.loads(content)
            if isinstance(parsed, list):
                return parsed
            return [parsed]
        except Exception:
            pass

        logs = []
        decoder = json.JSONDecoder()
        idx = 0
        length = len(content)
        while idx < length:
            while idx < length and content[idx] in " \t\r\n":
                idx += 1
            if idx >= length:
                break
            try:
                obj, end = decoder.raw_decode(content, idx)
                logs.append(obj)
                idx = end
                continue
            except json.JSONDecodeError:
                idx += 1
                continue

        if logs:
            return logs

        for line in content.splitlines():
            line = line.strip()
            if not line:
                continue
            if line.endswith(","):
                line = line[:-1]
            try:
                logs.append(json.loads(line))
            except Exception:
                continue
        if logs:
            return logs

        s = content.strip()
        if s.startswith("[") and not s.endswith("]"):
            depth_array = 0
            depth_object = 0
            last_top_level_comma = -1
            for i, ch in enumerate(s):
                if ch == "[":
                    depth_array += 1
                elif ch == "]":
                    depth_array -= 1
                elif ch == "{":
                    depth_object += 1
                elif ch == "}":
                    depth_object -= 1
                elif ch == "," and depth_array == 1 and depth_object == 0:
                    last_top_level_comma = i
                if depth_array == 0 and i > 0:
                    try:
                        parsed = json.loads(s[: i + 1])
                        if isinstance(parsed, list):
                            return parsed
                    except Exception:
                        break
            if last_top_level_comma != -1:
                candidate = s[: last_top_level_comma] + "]"
                try:
                    parsed = json.loads(candidate)
                    if isinstance(parsed, list):
                        return parsed
                except Exception:
                    pass

        return []

    try:
        content = await asyncio.to_thread(lambda: open(file, "r").read())
    except FileNotFoundError:
        return jsonify({"logs": [], "error": "log file not found"}), 200
    except Exception as e:
        return jsonify({"logs": [], "error": str(e)}), 200

    logs = _lenient_parse_json(content)
    return jsonify({"logs": logs}), 200


guid_fail_cache = {}


@utils_bp.post("/check")
async def check():
    if not request.is_json:
        return jsonify({"error": "Content-Type must be application/json"}), 415
    try:
        data = await request.get_json()
        incoming_guid = data.get("uuid")
        if not incoming_guid:
            return jsonify({"error": "Missing 'uuid'"}), 422

        print(f"Checking for guid: {incoming_guid}")

        if incoming_guid in guid_fail_cache:
            if guid_fail_cache[incoming_guid] >= 10:
                print(f"Guid: {incoming_guid} has failed 5 times, returning processed despite its non-existent entry")
                return jsonify({
                    "processed": True,
                    "status": "processed",
                    "uuid": incoming_guid
                }), 200
            guid_fail_cache[incoming_guid] += 1
        else:
            guid_fail_cache[incoming_guid] = 1

        def find_entry(guid: str):
            session_local = get_db_session()
            try:
                entry = session_local.query(Drop).filter(Drop.unique_id == guid,
                                                        Drop.used_api == True,
                                                        Drop.date_added > datetime.now() - timedelta(hours=12)).first()
                if entry:
                    return entry, "drop"
                entry = session_local.query(CollectionLogEntry).filter(CollectionLogEntry.unique_id == guid,
                                                                        CollectionLogEntry.used_api == True,
                                                                        CollectionLogEntry.date_added > datetime.now() - timedelta(hours=12)).first()
                if entry:
                    return entry, "collection_log"
                entry = session_local.query(PersonalBestEntry).filter(PersonalBestEntry.unique_id == guid,
                                                                        PersonalBestEntry.used_api == True,
                                                                        PersonalBestEntry.date_added > datetime.now() - timedelta(hours=12)).first()
                if entry:
                    return entry, "personal_best"
                entry = session_local.query(CombatAchievementEntry).filter(CombatAchievementEntry.unique_id == guid,
                                                                            CombatAchievementEntry.used_api == True,
                                                                            CombatAchievementEntry.date_added > datetime.now() - timedelta(hours=12)).first()
                if entry:
                    return entry, "combat_achievement"
                return None, None
            finally:
                try:
                    session_local.close()
                except Exception:
                    pass

        try:
            db_entry, entry_type = await asyncio.wait_for(
                asyncio.to_thread(find_entry, incoming_guid), timeout=3.0
            )
        except asyncio.TimeoutError:
            print(f"/check lookup timed out for guid: {incoming_guid}")
            return jsonify({
                "processed": False,
                "status": "timeout",
                "uuid": incoming_guid
            }), 200

        if not db_entry:
            print("No database entry found for guid: " + str(incoming_guid))
            return jsonify({
                "processed": False,
                "status": "not_found",
                "uuid": incoming_guid
            }), 200

        payload = {"processed": True, "status": "processed", "uuid": incoming_guid, "type": entry_type}
        print("Returning payload: " + str(payload))
        if entry_type == "drop":
            payload["id"] = getattr(db_entry, "drop_id", None)
        elif entry_type == "collection_log":
            payload["id"] = getattr(db_entry, "log_id", None)
        elif entry_type == "personal_best":
            payload["id"] = getattr(db_entry, "id", None)
        elif entry_type == "combat_achievement":
            payload["id"] = getattr(db_entry, "id", None)

        return jsonify(payload), 200
    except Exception as e:
        print(f"/check error: {e}")
        return jsonify({"error": "Malformed or invalid request"}), 400


@utils_bp.get("/metrics")
async def get_metrics():
    return jsonify(metrics.get_stats())


