from datetime import datetime
from typing import List
import asyncio

from quart import Blueprint, jsonify, request
from quart_cors import route_cors
from sqlalchemy import or_, text

from api.core import get_db_session, redis_client
from api.routes.helpers import assemble_submission_data
from services.redis_updates import get_player_list_loot_sum
from utils.format import format_number
from utils.wiseoldman import fetch_group_members
from db import Player, Group, GroupConfiguration, NotifiedSubmission, NpcList, get_current_partition
from db.ops import associate_player_ids
from utils.redis import calculate_rank_amongst_groups


groups_bp = Blueprint("groups", __name__)


@groups_bp.get("/top_groups")
async def top_groups():
    db_session = get_db_session()
    try:
        groups = db_session.query(Group).all()

        group_totals = {}
        for group_object in groups:
            group_id = group_object.group_id
            if group_id == 2 or group_id == 0:
                continue
            players_in_group = db_session.query(Player.player_id).join(Player.groups).filter(Group.group_id == group_id).all()
            group_totals[group_id] = 0
            try:
                group_month_total = get_player_list_loot_sum([player.player_id for player in players_in_group])
                group_totals[group_id] = group_month_total
            except Exception as e:
                print(f"Error getting group total for group {group_id}: {e}")
                group_totals[group_id] = 0

        sorted_groups = sorted(group_totals.items(), key=lambda x: x[1], reverse=True)
        final_groups = []
        for rank, (g_id, group_total) in enumerate(sorted_groups, start=1):
            group_object = db_session.query(Group).filter(Group.group_id == g_id).first()
            if g_id != 2 and g_id != 0:
                top_player_data = redis_client.client.zrevrange(
                    f"leaderboard:{get_current_partition()}:group:{g_id}",
                    0,
                    0,
                    withscores=True
                )

                top_player_display = None
                if top_player_data:
                    player_id_raw, player_score = top_player_data[0]
                    try:
                        player_id_int = int(player_id_raw.decode("utf-8")) if isinstance(player_id_raw, (bytes, bytearray)) else int(player_id_raw)
                    except Exception:
                        player_id_int = int(player_id_raw)
                    top_player = db_session.query(Player).filter(Player.player_id == player_id_int).first()
                    if top_player:
                        top_player_display = f"{top_player.player_name}"

                final_groups.append({
                    "group_name": group_object.group_name,
                    "total_loot": format_number(group_total),
                    "rank": rank,
                    "group_id": group_object.group_id,
                    "member_count": len(players_in_group),
                    "top_player": top_player_display,
                })
        return jsonify({"groups": final_groups}), 200
    finally:
        db_session.close()


@groups_bp.get("/group_search")
async def group_search():
    group_name = request.args.get("name", None)
    if not group_name:
        return jsonify({"error": "Group name is required"}), 400

    db_session = get_db_session()
    try:
        group: Group = db_session.query(Group).filter(Group.group_name == group_name).first()
        if not group:
            return jsonify({"error": "Group " + group_name + " not found"}), 404
        group_wom_id = db_session.query(Group.wom_id).filter(Group.group_id == group.group_id).first()
        wom_member_list = []
        try:
            if group_wom_id:
                group_wom_id = group_wom_id[0]
            if group_wom_id:
                wom_member_list = await fetch_group_members(wom_group_id=int(group_wom_id), session_to_use=db_session)
        except Exception as e:
            print("Couldn't get the member list", e)

        player_ids = await associate_player_ids(wom_member_list,session_to_use=db_session)
        group_rank, total_groups = calculate_rank_amongst_groups(group.group_id, player_ids, session_to_use=db_session)
        top_player_data = redis_client.client.zrevrange(
            f"leaderboard:{get_current_partition()}:group:{group.group_id}",
            0,
            0,
            withscores=True
        )

        top_player_display = None
        if top_player_data:
            player_id_raw, player_score = top_player_data[0]
            try:
                player_id_int = int(player_id_raw.decode("utf-8")) if isinstance(player_id_raw, (bytes, bytearray)) else int(player_id_raw)
            except Exception:
                player_id_int = int(player_id_raw)
            top_player = db_session.query(Player).filter(Player.player_id == player_id_int).first()
            if top_player:
                top_player_display = f"{top_player.player_name}"

        player_count = group.get_player_count(session_to_use=db_session)
        group_recent_submissions = db_session.query(NotifiedSubmission).filter(or_(NotifiedSubmission.pb_id != None, NotifiedSubmission.drop_id != None, NotifiedSubmission.clog_id != None)).filter(NotifiedSubmission.group_id == group.group_id).order_by(NotifiedSubmission.date_added.desc()).limit(10).all()
        final_submission_data = await assemble_submission_data(group_recent_submissions, db_session)
        players_in_group = db_session.query(Player.player_id).join(Player.groups).filter(Group.group_id == group.group_id).all()
        group_total = get_player_list_loot_sum([player.player_id for player in players_in_group])
        return jsonify({
            "group_name": group.group_name,
            "group_description": group.description,
            "group_image_url": group.icon_url,
            "public_discord_link": group.invite_url if group.invite_url else None,
            "group_droptracker_id": group.group_id,
            "group_members": player_count,
            "group_rank": f"{group_rank}/{total_groups}",
            "group_top_player": top_player_display,
            "group_recent_submissions": final_submission_data,
            "group_stats": {
                "total_members": player_count,
                "global_rank": f"{group_rank}/{total_groups}",
                "monthly_loot": format_number(group_total),
            },
        })
    finally:
        db_session.close()


@groups_bp.get("/groups/board_update/<int:group_id>")
@route_cors(allow_origin="https://www.droptracker.io")
async def group_board_update(group_id: int):
    try:
        force_raw = request.args.get("force", "false").lower()
        force = force_raw in ("1", "true", "yes", "y", "on")

        import sys
        from asyncio.subprocess import PIPE

        cmd = [sys.executable, "/store/droptracker/disc/board_cli.py", str(group_id)]
        if force:
            cmd.append("--force")

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=PIPE,
            stderr=PIPE
        )
        stdout, stderr = await proc.communicate()
        status_code = 200 if proc.returncode == 0 else 500

        return jsonify({
            "message": "Board update completed" if proc.returncode == 0 else "Board update failed",
            "group_id": group_id,
            "force": force,
            "returncode": proc.returncode,
            "stdout": stdout.decode(errors="ignore"),
            "stderr": stderr.decode(errors="ignore"),
        }), status_code
    except Exception as e:
        return jsonify({"error": f"Failed to run board update: {e}"}), 500

