import asyncio
from datetime import datetime, timedelta
import os
import time

from sqlalchemy import text
from db import (models, Group, GroupConfiguration, GroupPersonalBestMessage, NpcList, PersonalBestEntry, Player, session)
from utils.redis import redis_client, calculate_global_overall_rank, calculate_rank_amongst_groups
from utils.format import convert_from_ms, format_number
import interactions
from interactions import Embed
from dotenv import load_dotenv  

load_dotenv()

global_footer = os.getenv("DISCORD_MESSAGE_FOOTER")

async def get_global_drop_embed(item_name, item_id, player_id, quantity, value, npc_id):
    player = session.query(Player).filter(Player.player_id == player_id).first()
    groups = [group for group in player.groups if group.group_id != 2]
    top_group = None
    top_group_total = 0
    for group in groups:
        group_total = group.get_current_total()
        if group_total > top_group_total:
            top_group_total = group_total
            top_group = group
    formatted_item_name = item_name.replace(" ", "_")
    wiki_url = f"https://oldschool.runescape.wiki/w/{formatted_item_name}"
    embed = Embed(title=f"{item_name}",
                  url=wiki_url,
                  description=f"G/E Value: `{format_number(value * quantity)}`",
                  color=0x00ff00)
    partition = datetime.now().year * 100 + datetime.now().month
    current_month_string = datetime.now().strftime("%B")
    current_month = current_month_string.capitalize()
    player_total_key = f"player:{player_id}:{partition}:total_loot"
    player_total = redis_client.client.get(player_total_key)
    player_total_form = format_number(player_total)
    global_rank, ranked_global = calculate_global_overall_rank(player_id)
    embed.add_field(name="Player Stats", value=f"{current_month_string} Total: `{player_total_form}`\n" + 
                    f"Global Rank: `{global_rank}`/`{ranked_global}`")
    if top_group:
        group_to_group_rank, total_groups = calculate_rank_amongst_groups(top_group.group_id, [])
        embed.add_field(name=f"{top_group.group_name} Stats", value=f"{current_month} Total: `{format_number(top_group_total)}`\n" + 
                        f"Group Rank: `{group_to_group_rank}`/`{total_groups}`")
        icon_url = "https://www.droptracker.io/img/droptracker-small.gif" if top_group.icon_url is None else top_group.icon_url
    else:
        icon_url = "https://www.droptracker.io/img/droptracker-small.gif"
    embed.set_author(name=f"{player.player_name}",icon_url=icon_url)
    embed.set_thumbnail(url=f"https://www.droptracker.io/img/itemdb/{item_id}.png")
    embed.set_footer(global_footer)
    return embed

async def create_boss_pb_embed(group_id, boss_name, max_entries):
    npc_id = session.query(NpcList.npc_id).filter(NpcList.npc_name == boss_name).first()
    npc_id = npc_id[0] if npc_id else None
    embed = Embed(
        title=f"🏆 {boss_name} Leaderboards 🏆",
        description="━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        color=0x9B0000  # Dark blood red color
    )
    embed.set_thumbnail(url=f"https://www.droptracker.io/img/npcdb/{npc_id}.png")
    
    # Get player IDs in the group
    query = """SELECT player_id FROM user_group_association WHERE group_id = :group_id"""
    player_ids_result = session.execute(text(query), {"group_id": group_id}).fetchall()
    player_ids = [pid[0] for pid in player_ids_result]
    
    # Get all personal bests for these players and this NPC
    all_pbs = session.query(PersonalBestEntry).filter(
        PersonalBestEntry.player_id.in_(player_ids),
        PersonalBestEntry.npc_id == npc_id
    ).all()
    
    # Count total PBs
    total_pbs = len(all_pbs)
    
    # Sort all PBs by time
    all_pbs.sort(key=lambda pb: pb.personal_best)
    
    # Find all unique team sizes
    unsorted_team_sizes = list(set(pb.team_size for pb in all_pbs))
    
    # Sort team sizes in chronological order: Solo/1, 2, 3, etc.
    team_sizes = sort_team_sizes(unsorted_team_sizes)
    
    # Group PBs by team size
    team_size_pbs = {}
    for size in team_sizes:
        team_size_pbs[size] = []
    
    # Important fix: Use max_entries or a default value if it's 0
    entries_to_show = max_entries if max_entries > 0 else 3
    
    for pb in all_pbs:
        if pb.team_size in team_size_pbs and len(team_size_pbs[pb.team_size]) < entries_to_show:
            team_size_pbs[pb.team_size].append(pb)
    
    # Find fastest overall
    fastest_overall = all_pbs[0] if all_pbs else None
    fastest_time_str = ""
    fastest_player_str = ""
    
    if fastest_overall:
        # Format time using convert_from_ms
        fastest_time_str = convert_from_ms(fastest_overall.personal_best)
        
        # Get player info
        player = session.query(Player).filter(Player.player_id == fastest_overall.player_id).first()
        if player:
            discord_id = None
            if player.user_id:
                discord_id_query = """SELECT discord_id FROM users WHERE user_id = :user_id"""
                discord_id_result = session.execute(text(discord_id_query), {"user_id": player.user_id}).first()
                if discord_id_result:
                    discord_id = discord_id_result[0]
            if int(group_id) != 2:
                if discord_id:
                    fastest_player_str = f"<@{discord_id}> (`{player.player_name}`)"
                else:
                    fastest_player_str = f"`{player.player_name}`"
            else:
                fastest_player_str = f"`{player.player_name}`"
    
    # Convert team size to display format for summary
    team_size_display = fastest_overall.team_size if fastest_overall else ""
    if team_size_display == "Solo" or team_size_display == "1":
        team_size_text = "Solo"
    elif "+" in str(team_size_display):
        team_size_text = team_size_display
    else:
        try:
            team_size_text = f"{int(team_size_display)}-man"
        except:
            team_size_text = team_size_display
    most_looted_player_id, most_looted_total = await get_current_top_rank_at_npc(npc_id, group_id)
    most_looted_player = session.query(Player).filter(Player.player_id == most_looted_player_id).first()
    if most_looted_player:
        if most_looted_player.user and group_id != 2:
            most_looted_discord_id = most_looted_player.user.discord_id
            most_looted_player_str = f"<@{most_looted_discord_id}> (`{most_looted_player.player_name}`)"
        else:
            most_looted_player_str = f"`{most_looted_player.player_name}`"
    else:
        most_looted_player_str = "N/A"
        most_looted_total = 0
    # Summary section
    embed.add_field(
        name="__📊 Summary__",
        value=f"• **Total PBs Tracked:** `{total_pbs}`\n"
              f"• **Fastest Time:** `{fastest_time_str}` ({team_size_text})\n"
              f"  ↳ by {fastest_player_str}\n"
              f"• **Most Loot:** `{format_number(most_looted_total)}` gp\n"
              f"  ↳ by {most_looted_player_str}\n"
              "━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        inline=False
    )
    
    # Function to format a leaderboard for a team size
    def format_leaderboard(team_size_entries):
        if not team_size_entries:
            return "━━━━━━━━━━━━━━━━\nNo entries yet\n━━━━━━━━━━━━━━━━"
            
        value = "━━━━━━━━━━━━━━━━\n"
        for i, pb in enumerate(team_size_entries):
            player = session.query(Player).filter(Player.player_id == pb.player_id).first()
            if player:
                discord_id = None
                if player.user_id:
                    discord_id_query = """SELECT discord_id FROM users WHERE user_id = :user_id"""
                    discord_id_result = session.execute(text(discord_id_query), {"user_id": player.user_id}).first()
                    if discord_id_result:
                        discord_id = discord_id_result[0]
                
                # Format time using convert_from_ms
                time_str = convert_from_ms(pb.personal_best)
                
                medal = "🥇" if i == 0 else "🥈" if i == 1 else "🥉" if i == 2 else "  "
                if int(group_id) != 2 and discord_id:
                    player_str = f"<@{discord_id}>"
                else:
                    player_str = f"`{player.player_name}`"
                value += f"{medal} {player_str} • `{time_str}`\n"
        value += "━━━━━━━━━━━━━━━━"
        return value
    
    # Function to get the proper team size name
    def get_team_size_name(size):
        if size == "Solo" or size == "1":
            return "__Solo__"
        elif "+" in str(size):
            return f"__{size}__"
        else:
            try:
                size_int = int(size)
                if size_int >= 6:
                    return f"__{size_int}-man__"
                else:
                    return f"__{size_int}-man__"
            except:
                return f"__{size}__"
    
    # Add team size leaderboards in pairs
    team_sizes_to_display = team_sizes[:8]  # Limit to 8 team sizes max
    
    # Process team sizes in pairs
    for i in range(0, len(team_sizes_to_display), 2):
        # First team size in the pair
        size1 = team_sizes_to_display[i]
        name1 = get_team_size_name(size1)
        value1 = format_leaderboard(team_size_pbs[size1])
        embed.add_field(name=name1, value=value1, inline=True)
        
        # Second team size in the pair (if exists)
        if i + 1 < len(team_sizes_to_display):
            size2 = team_sizes_to_display[i + 1]
            name2 = get_team_size_name(size2)
            value2 = format_leaderboard(team_size_pbs[size2])
            embed.add_field(name=name2, value=value2, inline=True)
        else:
            # Add an empty field to maintain layout
            embed.add_field(name="\u200b", value="\u200b", inline=True)
        
        # Add a spacer after each pair (except the last)
        if i < len(team_sizes_to_display) - 2:
            embed.add_field(name="\u200b", value="\u200b", inline=False)
    
    # Footer
    embed.set_footer(text="Powered by the DropTracker | https://www.droptracker.io")
    
    return embed

def sort_team_sizes(team_sizes):
    """
    Sort team sizes in chronological order: Solo/1, 2, 3, etc.
    
    Args:
        team_sizes: List of team size strings (e.g. ["5", "Solo", "3", "2"])
        
    Returns:
        Sorted list with "Solo" first, followed by numeric sizes in ascending order
    """
    result = []
    has_solo = False
    numeric_sizes = []
    special_sizes = []  # For sizes with "+" or other non-numeric formats
    
    for size in team_sizes:
        # Check if this is a "Solo" entry
        if str(size).lower() == "solo" or str(size) == "1":
            has_solo = True
        elif "+" in str(size):
            # Handle special formats like "5+"
            special_sizes.append(size)
        else:
            try:
                # Try to convert to integer for sorting
                numeric_sizes.append(int(size))
            except ValueError:
                # If it can't be converted, add to special sizes
                special_sizes.append(size)
    
    # Sort numeric sizes
    numeric_sizes.sort()
    
    # Sort special sizes (this will sort alphanumerically)
    special_sizes.sort()
    
    # Add "Solo" first if it exists
    if has_solo:
        result.append("Solo")
    
    # Add the numeric sizes
    for size in numeric_sizes:
        # Don't add "1" if we already added "Solo"
        if not (has_solo and size == 1):
            result.append(str(size))
    
    # Add the special sizes at the end
    result.extend(special_sizes)
    
    return result

async def get_current_top_rank_at_npc(npc_id, group_id):
    npc = session.query(NpcList).filter(NpcList.npc_id == npc_id).first()
    npc_name = npc.npc_name if npc else f"Unknown NPC ({npc_id})"
    group = session.query(Group).filter(Group.group_id == group_id).first()
    query = """SELECT player_id FROM user_group_association WHERE group_id = :group_id"""
    player_ids_result = session.execute(text(query), {"group_id": group_id}).fetchall()
    player_ids = [pid[0] for pid in player_ids_result]
    player_totals = {}
    partition = datetime.now().year * 100 + datetime.now().month
    
    for player_id in player_ids:
        # Use hget instead of get to retrieve the specific NPC total from the hash
        player_total = redis_client.client.hget(f"player:{player_id}:{partition}:npc_totals", str(npc_id))
        if player_total:
            player_totals[player_id] = int(player_total.decode('utf-8'))
        else:
            player_totals[player_id] = 0
    
    # Handle case where no players have totals
    if not player_totals:
        return None, 0
        
    sorted_totals = sorted(player_totals.items(), key=lambda x: x[1], reverse=True)
    top_player = sorted_totals[0][0]
    top_player_total = sorted_totals[0][1]
    return top_player, top_player_total

async def update_boss_pb_embed(bot: interactions.Client, group_id, npc_id, from_submission: bool = False):
    ## Returns a tuple of two booleans:
    ## - Whether the embed refresh was successfully processed
    ## - Whether or not we should wait for a rate limit before continuing if we have more to process.
    group = session.query(Group).filter(Group.group_id == group_id).first()
    npc = session.query(NpcList).filter(NpcList.npc_id == npc_id).first()
    npc_name = npc.npc_name if npc else f"Unknown NPC ({npc_id})"
    existing_message = session.query(GroupPersonalBestMessage).filter(
        GroupPersonalBestMessage.group_id == group.group_id, 
        GroupPersonalBestMessage.boss_name == npc_name
    ).first()
    max_entries = session.query(GroupConfiguration.config_value).filter(
        GroupConfiguration.group_id == group.group_id,
        GroupConfiguration.config_key == 'number_of_pbs_to_display'
    ).first()
    
    max_entries = int(max_entries.config_value) if max_entries else 3
    if existing_message:
        ## More delays :)
        await asyncio.sleep(3)
        try:
            existing_message_id = existing_message.message_id
            channel_id = existing_message.channel_id
            channel = await bot.fetch_channel(channel_id=int(channel_id), force=True)
            existing_message_obj = await channel.fetch_message(existing_message_id)
            
            if existing_message_obj:
                pb_embed: Embed = await create_boss_pb_embed(group.group_id, npc_name, max_entries)
                existing_embed = existing_message_obj.embeds[0]
                has_refresh = False
                for field in existing_embed.fields:
                    if field.name == "Last updated:":
                        has_refresh = True
                if embeds_are_equal(pb_embed, existing_embed) and has_refresh:
                    return True, False
                next_update = datetime.now() + timedelta(minutes=30)
                future_timestamp = int(time.mktime(next_update.timetuple()))
                now = datetime.now()
                now_timestamp = int(time.mktime(now.timetuple()))
                if not from_submission:
                    pb_embed.add_field(name="Last updated:", value=f"<t:{now_timestamp}:R>", inline=False)
                else:
                    existing_embed = existing_message_obj.embeds[0]
                    refresh_value = None
                    for field in existing_embed.fields:
                        if field.name == "Last updated:":
                            refresh_value = f"<t:{now_timestamp}:R>"
                    if refresh_value:
                        pb_embed.add_field(name="Last updated:", value=refresh_value, inline=False)
                await existing_message_obj.edit(embed=pb_embed)
                existing_message.date_updated = datetime.now()
                session.commit()
                
                # Increment message count and handle rate limits
                return True, True
            else:
                return False, False
        except Exception as e:
            await asyncio.sleep(5)  # Wait on error to be safe
            message_count = 0
            return False, True
    else:
        return False, False
    

def embeds_are_equal(embed1: Embed, embed2: Embed):
    if embed1.title != embed2.title or embed1.description != embed2.description or embed1.color != embed2.color:
        return False
    
    # Compare fields (excluding the "Next refresh" field which will always be different)
    if len(embed1.fields) != len(embed2.fields):
        return False
    
    for i in range(len(embed1.fields) - 1):  # Skip the last field (refresh time)
        field1 = embed1.fields[i]
        field2 = embed2.fields[i]
        if field1.name != field2.name or field1.value != field2.value or field1.inline != field2.inline:
            return False
    return True