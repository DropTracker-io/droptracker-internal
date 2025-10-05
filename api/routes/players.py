from datetime import datetime

from quart import Blueprint, jsonify, request
from quart_rate_limiter import rate_limit
from datetime import timedelta
from sqlalchemy import or_, text

from api.core import get_db_session, redis_client, redis_tracker
from api.routes.helpers import assemble_submission_data
from utils.format import format_number
from services.redis_updates import get_player_current_month_total
from data.TOP_NPCS import TOP_NPCS
from db import Player, NotifiedSubmission, NpcList, Group, GroupConfiguration, get_current_partition
from utils.wiseoldman import check_user_by_username


players_bp = Blueprint("players", __name__)


@players_bp.get("/player_search")
async def player_search():
    player_name = request.args.get("name", None)
    if not player_name:
        return jsonify({"error": "Player name is required"}), 400

    db_session = get_db_session()
    try:
        player = db_session.query(Player).filter(Player.player_name == player_name).first()
        if not player:
            player = db_session.query(Player).filter(Player.player_name.ilike(f"%{player_name}%")).first()

        if not player:
            return jsonify({"error": f"Player '{player_name}' not found"}), 404

        player_recent_submissions = db_session.query(NotifiedSubmission).filter(or_(NotifiedSubmission.pb_id != None, NotifiedSubmission.drop_id != None, NotifiedSubmission.clog_id != None)).filter(NotifiedSubmission.player_id == player.player_id).order_by(NotifiedSubmission.date_added.desc()).limit(10).all()
        final_submission_data = await assemble_submission_data(player_recent_submissions, db_session)
        player_loot = get_player_current_month_total(player.player_id)
        player_rank = redis_tracker.get_player_rank(player.player_id)
        if player_rank is not None:
            player_rank += 1
        player_npc_ranks = {}
        target_npcs = db_session.query(NpcList).filter(NpcList.npc_id.in_(TOP_NPCS)).all()
        for npc in target_npcs:
            player_npc_rank, player_npc_score = player.get_score_at_npc(npc.npc_id)
            player_npc_ranks[npc.npc_name] = {"rank": player_npc_rank, "loot": format_number(player_npc_score)}
        if len(player_npc_ranks) == 0:
            top_npc_name = "Unknown"
            top_npc_data = {"rank": 0, "loot": 0}
            top_npc_data["name"] = top_npc_name
        else:
            top_npc_name = max(player_npc_ranks, key=lambda x: player_npc_ranks[x]["loot"]) 
            top_npc_data = player_npc_ranks[top_npc_name].copy()
            top_npc_data["name"] = top_npc_name
        player_group_id_query = """SELECT group_id FROM user_group_association WHERE player_id = :player_id"""
        player_group_ids_result = db_session.execute(text(player_group_id_query), {"player_id": player.player_id}).fetchall()
        player_group_ids = [g[0] for g in player_group_ids_result if g[0] > 2]
        player_groups = []
        for gid in player_group_ids:
            group_object: Group = db_session.query(Group).filter(Group.group_id == gid).first()
            player_groups.append({"name": group_object.group_name, 
                                  "id": gid,
                                  "loot": format_number(group_object.get_current_total()),
                                  "members": group_object.get_player_count(session_to_use=db_session)})
        player_groups.sort(key=lambda x: x["loot"], reverse=True)
        from services.points import get_player_lifetime_points_earned
        player_lifetime_points = get_player_lifetime_points_earned(player_id=player.player_id,session=db_session)

        response_data = {
            "player_name": player.player_name,
            "droptracker_player_id": player.player_id,
            "registered": True if player.user_id else False,
            "total_loot": format_number(player_loot),
            "global_rank": player_rank,
            "top_npc": top_npc_data,
            "best_pb_rank": 42,
            "points": player_lifetime_points,
            "groups": player_groups,
            "recent_submissions": final_submission_data,
        }

        return jsonify(response_data), 200
    finally:
        db_session.close()


@players_bp.get("/top_players")
async def top_players():
    db_session = get_db_session()
    try:
        top_players = redis_client.client.zrevrange(
            f"leaderboard:{get_current_partition()}",
            0,
            4,
            withscores=True
        )

        top_players_data = []
        for rank, (player_id, player_score) in enumerate(top_players, start=1):
            player = db_session.query(Player).filter(Player.player_id == player_id).first()
            top_players_data.append({
                "rank": rank,
                "player_name": player.player_name,
                "total_loot": format_number(player_score)
            })
        return jsonify({"players": top_players_data}), 200
    except Exception:
        return jsonify({"error": "Internal server error"}), 500
    finally:
        db_session.close()


@players_bp.get("/player")
@rate_limit(limit=5, period=timedelta(seconds=10))
async def get_player():
    player_name = request.args.get("player_name")
    if not player_name:
        return jsonify({"error": "Player name is required"}), 400

    db_session = get_db_session()
    try:
        player = db_session.query(Player).filter(Player.player_name == player_name).first()
        if not player:
            return jsonify({"error": "Player not found"}), 404
        return jsonify({"player": player.to_dict()})
    finally:
        db_session.close()


@players_bp.get("/load_config")
async def load_config():
    player_name = request.args.get("player_name", None)
    acc_hash = request.args.get("acc_hash", None)
    if not player_name or not acc_hash:
        return jsonify({"error": "Player name and acc_hash are required"}), 400
    db_session = get_db_session()
    try:
        player = db_session.query(Player).filter(Player.player_name == player_name, Player.account_hash == acc_hash).first()
        if not player:
            try:
                player_wom_data = await check_user_by_username(player_name)
                player = Player(player_name=player_name, account_hash=acc_hash, wom_id=player_wom_data[2], log_slots=player_wom_data[3])
                db_session.add(player)
                db_session.commit()
            except Exception as e:
                return jsonify({"error": "Error checking user by username: " + str(e)}), 500
            return jsonify({"error": "Player not found"}), 404
        player_gids = db_session.execute(text("SELECT group_id FROM user_group_association WHERE player_id = :player_id"), {"player_id": player.player_id}).all()
        group_configs = []
        def get_config_value(current_group_configs, key: str):
            for group_config in current_group_configs:
                if group_config.config_key == key:
                    if key == "level_minimum_for_notifications":
                        return group_config.config_value
                    config_val = group_config.config_value if group_config.config_value and group_config.config_value != "" else group_config.long_value
                    if config_val == "true" or config_val == "1":
                        return True
                    elif config_val == "false" or config_val == "0":
                        return False
                    elif config_val == "":
                        return None
                    return config_val
            return ""
        for group_id_row in player_gids:
            group_id = group_id_row[0]
            group = db_session.query(Group).filter(Group.group_id == group_id).first()
            current_group_configs = db_session.query(GroupConfiguration).filter(GroupConfiguration.group_id == group_id).all()
            group_configs.append({"group_id": group_id,
                                "group_name": group.group_name,
                                "min_value": get_config_value(current_group_configs, "minimum_value_to_notify"),
                                "minimum_drop_value": get_config_value(current_group_configs, "minimum_value_to_notify"),
                                "only_screenshots": get_config_value(current_group_configs, "only_send_messages_with_images"),
                                "send_drops": True,
                                "send_pbs": get_config_value(current_group_configs, "notify_pbs"),
                                "send_clogs": get_config_value(current_group_configs, "notify_clogs"),
                                "send_cas": get_config_value(current_group_configs, "notify_cas"),
                                "send_pets": get_config_value(current_group_configs, "send_pets"),
                                "send_xp": get_config_value(current_group_configs, "notify_levels"),
                                "minimum_level": get_config_value(current_group_configs, "level_minimum_for_notifications"),
                                "send_stacked_items": get_config_value(current_group_configs, "send_stacks_of_items"),
                                "minimum_ca_tier": get_config_value(current_group_configs, "min_ca_tier_to_notify")})
        return jsonify(group_configs), 200
    finally:
        db_session.close()


