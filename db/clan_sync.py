from interactions import Task, IntervalTrigger
from db.models import Group, session
from sqlalchemy import text
import asyncio

async def insert_xf_group(group: Group):
    existing_xf_group = session.execute(
        text("SELECT * FROM xenforo.dt_player_group WHERE group_id = :group_id"),
        {"group_id": group.group_id}
    ).first()
    if existing_xf_group:
        pass
    else:
        new_group_data = {
            "group_id": group.group_id,
            "group_name": group.group_name,
            "wom_id": group.wom_id,
            "guild_id": group.guild_id,
            "group_description": group.description,
            "group_icon": group.icon_url if group.icon_url else "https://www.droptracker.io/img/droptracker-small.gif",
            "discord_url": group.invite_url if group.invite_url else "",
            "date_added": int(group.date_added.timestamp()),
            "date_updated": int(group.date_updated.timestamp()),
            "current_total": 0,
        }

        session.execute(
            text("INSERT INTO xenforo.dt_player_group (group_id, group_name, wom_id, guild_id, group_description, group_icon, discord_url, date_added, date_updated, current_total) VALUES (:group_id, :group_name, :wom_id, :guild_id, :group_description, :group_icon, :discord_url, :date_added, :date_updated, :current_total)"),
            new_group_data
        )
        session.commit()
        
        print(f"Inserted a new group into the XenForo database: {group.group_name}")