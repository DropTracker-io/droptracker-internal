import asyncio
import csv
import calendar
import json
import os
import time
import traceback
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

from sqlalchemy import and_, select
from db.models import Drop, Guild, IgnoredPlayer, LootboardStyle, Player, ItemList, Session, session, Group, GroupConfiguration, NpcList
from db import models
from io import BytesIO

import aiohttp
import interactions
from PIL import Image, ImageFont, ImageDraw

from utils.redis import RedisClient
from utils.wiseoldman import fetch_group_members
from db.ops import DatabaseOperations, associate_player_ids

from utils.format import format_number
from utils.dynamic_handling import get_value_color, get_dynamic_color, get_coin_image_id

redis_client = RedisClient()
db = DatabaseOperations()
yellow = (255, 255, 0)
black = (0, 0, 0)
font_size = 26

rs_font_path = "static/assets/fonts/runescape_uf.ttf"
tracker_fontpath = 'static/assets/fonts/droptrackerfont.ttf'
main_font = ImageFont.truetype(rs_font_path, font_size)

def get_db_session():
    """Get a fresh session using the existing session factory"""
    return Session()

async def get_drops_for_group_optimized(player_ids: List[int], partition: str, group_id: int = None) -> Tuple[Dict, Dict, List, int]:
    """
    Optimized version of get_drops_for_group using suggested improvements
    """
    verbose = False
    from collections import defaultdict
    import heapq
    
    overall_start = time.time()
    
    # Use defaultdict to avoid key existence checks
    group_items = defaultdict(lambda: [0, 0])  # [quantity, total_value]
    player_totals = defaultdict(int)
    recent_drops_heap = []  # Min heap for top 100 drops by date
    total_loot = 0
    
    # Counter for heap tie-breaking when timestamps are equal
    drop_counter = 0
    
    # Partition processing
    is_daily = len(str(partition)) > 6 or '-' in str(partition)
    partition_int = int(partition.replace('-', '')) if is_daily and '-' in str(partition) else int(partition)
    
    if verbose:
        print(f"\n[OPTIMIZED] Processing {len(player_ids)} players with partition {partition_int}")
    
    with get_db_session() as session:
        # Single query for all players
        query_start = time.time()
        
        # Fetch all drops in one query
        drops_query = select(
            Drop.drop_id,
            Drop.player_id,
            Drop.item_id,
            Drop.quantity,
            Drop.value,
            Drop.date_added,
            Drop.npc_id
        ).where(
            and_(
                Drop.player_id.in_(player_ids),
                Drop.partition == partition_int,
                Drop.value.isnot(None),
                Drop.quantity.isnot(None),
                Drop.item_id.isnot(None)
            )
        )
        
        result = session.execute(drops_query)
        all_drops = result.fetchall()
        
        query_time = time.time() - query_start
        # Process drops
        process_start = time.time()
        if group_id:
            group_minimum_value = 500000
            try:
                group_minimum_value_raw = session.query(GroupConfiguration).filter(GroupConfiguration.group_id == group_id,
                                                                               GroupConfiguration.config_key == "minimum_value_to_notify").first().config_value
                group_minimum_value = int(group_minimum_value_raw)
            except:
                pass
            try:
                only_include_items_over_minimum = session.query(GroupConfiguration).filter(GroupConfiguration.group_id == group_id,
                                                                               GroupConfiguration.config_key == "only_include_items_over_minimum").first().config_value
                only_include_items_over_minimum = bool(only_include_items_over_minimum)
            except:
                only_include_items_over_minimum = False
        else:
            group_minimum_value = 500000
        for drop in all_drops:
            drop_id, player_id, item_id, quantity, value, date_added, npc_id = drop
            
            item_total_value = quantity * value
            
            # Check if we should only include items over minimum value
            if only_include_items_over_minimum and value < group_minimum_value:
                # Skip this item entirely if it's below minimum value
                continue
            
            # Update group items (using defaultdict)
            group_items[str(item_id)][0] += quantity
            group_items[str(item_id)][1] += item_total_value
            
            # Update player totals (using defaultdict)
            player_totals[player_id] += item_total_value
            
            # Track recent high-value drops using heap
            if value >= group_minimum_value:
                date_str = date_added.isoformat() if isinstance(date_added, datetime) else str(date_added)
                
                # For daily partitions, filter by date
                if not is_daily or not '-' in str(partition) or date_added.strftime('%Y-%m-%d') == partition:
                    # Use negative drop_id for max heap behavior (highest drop_id first)
                    heap_key = -drop_id  # Negative for max heap behavior
                    
                    drop_data = {
                        'drop_id': drop_id,
                        'item_id': item_id,
                        'player_id': player_id,
                        'value': value,
                        'quantity': quantity,
                        'date_added': date_str,
                        'npc_id': npc_id
                    }
                    
                    # Increment counter for tie-breaking
                    drop_counter += 1
                    
                    # Push all items to the heap regardless of its size; so we don't lose anything--we can sort later
                    heapq.heappush(recent_drops_heap, (heap_key, drop_counter, drop_data))
        
        process_time = time.time() - process_start
        
        # Convert defaultdicts to regular dicts with proper format
        format_start = time.time()
        group_items_formatted = {
            item_id: f"{values[0]},{values[1]}"
            for item_id, values in group_items.items()
        }
        player_totals_dict = dict(player_totals)
        total_loot = sum(player_totals.values())
        
        # Extract recent drops from heap and sort by drop_id descending (most recent first)
        # Extract the drop_data (3rd element) from each tuple and sort by drop_id descending
        recent_drops = [drop[2] for drop in sorted(recent_drops_heap)]
        
        format_time = time.time() - format_start
    
    overall_time = time.time() - overall_start
    
    
    return group_items_formatted, player_totals_dict, recent_drops, total_loot

async def get_drops_for_group(player_ids, partition: str, only_include_items_over_minimum: bool = False, group_minimum_value: int = 500000):
    """ Returns the drops stored in redis cache 
        for the specific list of player_ids
    """
    group_items = {}
    recent_drops = []
    total_loot = 0
    player_totals = {}
    
    # Determine if we're using a daily partition (contains a dash) or monthly partition
    
    async def process_player(player_id):
        nonlocal total_loot
        player_total = 0
        player_total_included = 0
        
        # Use different key format based on partition type
        if len(str(partition)) > 6:
            ## Anything with a partition greater than 6 digits is a special type...
            ## A "Daily" partition would come in as 20250327
            ## An "hourly" partition would come in as 2025032712 (12th hour of the 27th of March 2025)
            ## A "minute" partition would come in as 202503271234 (12:34th minute of the 27th of March 2025)
            ## Here, we split them into year, month, day
            ## We are using something more specific than the daily partitions...
            pass
        else:
            # Monthly format: player:123:202503:total_items
            total_items_key = f"player:{player_id}:{partition}:total_items"
            recent_items_key = f"player:{player_id}:{partition}:recent_items"
            loot_key = f"player:{player_id}:{partition}:total_loot"

        # Get total items
        total_items = redis_client.client.hgetall(total_items_key)
        
        for key, value in total_items.items():
            key = key.decode('utf-8')
            value = value.decode('utf-8')
            try:
                # Handle new 5-field format: quantity,total_value,drop_count,first_drop,last_drop
                parts = value.split(',')
                if len(parts) >= 2:
                    quantity = int(parts[0])
                    total_value = int(parts[1])
                    # If filtering is enabled, exclude items whose per-item value is below the minimum
                    if only_include_items_over_minimum:
                        if quantity <= 0:
                            continue
                        per_item_value = total_value // quantity
                        if per_item_value < int(group_minimum_value):
                            continue
                    # Ignore additional fields (drop_count, first_drop, last_drop) for board generation
                else:
                    continue
            except ValueError:
                continue
            
            if key in group_items:
                existing_quantity, existing_value = map(int, group_items[key].split(','))
                new_quantity = existing_quantity + quantity
                new_total_value = existing_value + total_value
                group_items[key] = f"{new_quantity},{new_total_value}"
            else:
                group_items[key] = f"{quantity},{total_value}"
            
            if only_include_items_over_minimum:
                player_total_included += total_value
                
        player_total = redis_client.client.get(loot_key)
        if player_total:
            player_total = int(player_total.decode('utf-8'))
        else:
            player_total = 0
        
        # If filtering is enabled, override the player's total to include only allowed items
        if only_include_items_over_minimum:
            player_total = player_total_included
            
        # Get recent items and ensure uniqueness
        if len(str(partition)) > 6:
            # Get all recent items first
            all_recent_items = [json.loads(item.decode('utf-8')) for item in redis_client.client.lrange(recent_items_key, 0, -1)]
            # Filter to only include items from the specified day
            target_date = partition  # Format: 2025-03-04
            recent_items = list({item['date_added']: item for item in all_recent_items 
                               if item['date_added'].startswith(target_date)}.values())
        else:
            recent_items = list({json.loads(item.decode('utf-8'))['date_added']: json.loads(item.decode('utf-8')) 
                            for item in redis_client.client.lrange(recent_items_key, 0, -1)}.values())
        
        return player_id, player_total, recent_items

    # Use asyncio.gather to process all players concurrently
    results = await asyncio.gather(*[process_player(player_id) for player_id in player_ids])

    for player_id, player_total, player_recent_items in results:
        player_totals[player_id] = player_total
        total_loot += player_total
        recent_drops.extend(player_recent_items)

    # Ensure recent drops are in chronological order (newest first)
    try:
        recent_drops.sort(key=lambda x: x.get('date_added', ''), reverse=True)
    except Exception:
        pass

    return group_items, player_totals, recent_drops, total_loot


async def get_generated_board_path(group_id: int = 0, wom_group_id: int = 0, partition: str = None):
    """
    Get the path to the generated board image.
    Used for the Discord bot to send/update lootboard images, since the generation method is called every 2 minutes by external processes.
    """
    current_date = datetime.now()
    ydmpart = int(current_date.strftime('%d%m%Y'))
    file_path = f"/store/droptracker/disc/static/assets/img/clans/{group_id}/lb/lootboard.png"
    return file_path

async def generate_server_board(group_id: int = 0, wom_group_id: int = 0, partition: str = None, session_to_use = None):
    """
        :param: bot: Instance of the interactions.Client bot object
        :param: group_id: DropTracker GroupID. 0 expects a wom_group_id
        :param: wom_group_id: WiseOldMan groupID. 0 expects a group_id
        :param: partition: The partition to search drops for (202408 for August 2024 or 2025-03-04 for daily)
        Providing neither option (group_id, wom_group_id) uses global drops.
    """
    # Set default partition if none provided
    if session_to_use is not None:
        session = session_to_use
    else:
        session = models.session
    if partition is None:
        partition = datetime.now().year * 100 + datetime.now().month
    
    # Determine if we're using a daily partition
    if len(str(partition)) > 6:
        ## Our normal partitioning always uses 6 digits -- YYYYMM
        is_daily = True
    else:
        is_daily = False
    
    group = None
    if group_id != 0: ## we prioritize group_id here
        group = session.query(Group).filter(Group.group_id == group_id).first()
    elif wom_group_id != 0:
        group = session.query(Group).filter(Group.wom_id == wom_group_id).first()
    
    if (group_id != 0 or wom_group_id != 0) and not group:
        print("Cannot generate a lootboard, no group data was properly parsed..")
    elif (group_id == 0 and wom_group_id == 0):
        group_id = 1
    else:
        group_id = group.group_id
    
    group_config = session.query(GroupConfiguration).filter(GroupConfiguration.group_id == group_id).all()
    # Transform group_config into a dictionary for easy access
    config = {conf.config_key: conf.config_value for conf in group_config}

    #loot_board_style = 1  # TODO: Implement other boards eventually
    loot_board_style = config.get('loot_board_type', 1)
    if loot_board_style == 0 or loot_board_style == "":
        loot_board_style = 1
    minimum_value = config.get('minimum_value_to_notify', 2500000)
    player_wom_ids = []
    # Load background image based on the board style
    loot_board_style = int(loot_board_style)
    target_board = session.query(LootboardStyle).filter(LootboardStyle.id == loot_board_style).first()
    if not target_board:
        target_board = session.query(LootboardStyle).filter(LootboardStyle.id == 1).first()
    local_url = target_board.local_url
    if not target_board:
        local_url = "/store/droptracker/disc/lootboard/themes/bank-new-clean-dark.png"

    bg_img, draw = load_background_image(local_url)

    # Compute the dynamic text color based on the background image. (- added BY Smoke [https://github.com/Varietyz/])
    use_dynamic_colors = config.get('use_dynamic_lootboard_colors', True)
    if use_dynamic_colors and use_dynamic_colors == "1":
        use_dynamic_colors = True
    else:
        use_dynamic_colors = False
    use_gp_colors = config.get('use_gp_colors', True)
    #print(f"Dynamic color selected: {dynamic_color}")
    
    #f"Group ID: {group_id}")
    if group_id != 2:
        if wom_group_id == 0:
            wom_group_id = group.wom_id
        elif wom_group_id != 0:
    # Fetch player WOM IDs and associated Player IDs
            player_wom_ids = await fetch_group_members(wom_group_id, session_to_use=session)
        else:
            player_wom_ids = []
            raw_wom_ids = session.query(Player.wom_id).all() ## get all users if no wom_group_id is found
            for player in raw_wom_ids:
                player_wom_ids.append(player[0])
    else:
        #print("Group ID is 2...")
        player_wom_ids = []
        all_players = session.query(Player.wom_id).all()
        #print(f"Got all players: {len(all_players)}")
        for p in all_players:
            player_wom_ids.append(p.wom_id)
    player_ids = await associate_player_ids(player_wom_ids, session_to_use=session)
    # Get the drops, recent drops, and total loot for the group
    #print("Processed player_ids")
    group_items, player_totals, recent_drops, total_loot = await get_drops_for_group_optimized(player_ids, partition, int(group.group_id))
    print("Got recent drops:", len(recent_drops))
    redis_client.client.zadd(f'gleaderboard:{partition}', {group.group_id: total_loot})
    with open(f"/store/droptracker/disc/static/assets/img/clans/{group_id}/recent_drops.json", "w") as f:
        json.dump(recent_drops, f, indent=4)
    # Draw elements on the background image (added dynamic_coloring - added BY Smoke [https://github.com/Varietyz/])
    bg_img = await draw_drops_on_image(bg_img, draw, group_items, group_id, dynamic_colors=use_dynamic_colors, use_gp=use_gp_colors)  # Pass `group_items` here
    bg_img = await draw_headers(group_id, total_loot, bg_img, draw, partition, dynamic_colors=use_dynamic_colors, use_gp=use_gp_colors)  # Draw headers
    bg_img = await draw_recent_drops(bg_img, draw, recent_drops, min_value=minimum_value, dynamic_colors=use_dynamic_colors, use_gp=use_gp_colors)  # Draw recent drops, with a minimum value
    bg_img = await draw_leaderboard(bg_img, draw, player_totals, dynamic_colors=use_dynamic_colors, use_gp=use_gp_colors)  # Draw leaderboard
    save_image(bg_img, group_id, partition)  # Save the generated image
    #print("Saved the new image.")
    
    # When saving the image, use a different naming convention for daily partitions 
    if is_daily:
        # For daily partitions, use the date directly
        file_path = f"/store/droptracker/disc/static/assets/img/clans/{group_id}/lb/daily_{partition.replace('-', '')}.png"
    else:
        # For monthly partitions, use the existing format
        current_date = datetime.now()
        ydmpart = int(current_date.strftime('%d%m%Y'))
        file_path = f"/store/droptracker/disc/static/assets/img/clans/{group_id}/lb/lootboard.png"
    
    return file_path



async def generate_server_board_temporary(group_id: int = 0, wom_group_id: int = 0, partition: str = None, session_to_use = None):
    """
        :param: bot: Instance of the interactions.Client bot object
        :param: group_id: DropTracker GroupID. 0 expects a wom_group_id
        :param: wom_group_id: WiseOldMan groupID. 0 expects a group_id
        :param: partition: The partition to search drops for (202408 for August 2024 or 2025-03-04 for daily)
        Providing neither option (group_id, wom_group_id) uses global drops.
    """
    # Set default partition if none provided
    if session_to_use is not None:
        session = session_to_use
    else:
        session = models.session
    if partition is None:
        partition = datetime.now().year * 100 + datetime.now().month
    
    # Determine if we're using a daily partition
    if len(str(partition)) > 6:
        ## Our normal partitioning always uses 6 digits -- YYYYMM
        is_daily = True
    else:
        is_daily = False
    
    group = None
    if group_id != 0: ## we prioritize group_id here
        group = session.query(Group).filter(Group.group_id == group_id).first()
    elif wom_group_id != 0:
        group = session.query(Group).filter(Group.wom_id == wom_group_id).first()
    
    if (group_id != 0 or wom_group_id != 0) and not group:
        print("Cannot generate a lootboard, no group data was properly parsed..")
    elif (group_id == 0 and wom_group_id == 0):
        group_id = 1
    else:
        group_id = group.group_id
    
    group_config = session.query(GroupConfiguration).filter(GroupConfiguration.group_id == group_id).all()
    # Transform group_config into a dictionary for easy access
    config = {conf.config_key: conf.config_value for conf in group_config}

    #loot_board_style = 1  # TODO: Implement other boards eventually
    loot_board_style = config.get('loot_board_type', 1)
    if loot_board_style == 0 or loot_board_style == "":
        loot_board_style = 1
    minimum_value = config.get('minimum_value_to_notify', 2500000)
    player_wom_ids = []
    # Load background image based on the board style
    loot_board_style = int(loot_board_style)
    target_board = session.query(LootboardStyle).filter(LootboardStyle.id == loot_board_style).first()
    if not target_board:
        target_board = session.query(LootboardStyle).filter(LootboardStyle.id == 1).first()
    local_url = target_board.local_url
    if not target_board:
        local_url = "/store/droptracker/disc/lootboard/themes/bank-new-clean-dark.png"

    bg_img, draw = load_background_image(local_url)

    # Compute the dynamic text color based on the background image. (- added BY Smoke [https://github.com/Varietyz/])
    use_dynamic_colors = config.get('use_dynamic_lootboard_colors', True)
    if use_dynamic_colors and use_dynamic_colors == "1":
        use_dynamic_colors = True
    else:
        use_dynamic_colors = False
    use_gp_colors = config.get('use_gp_colors', True)
    #print(f"Dynamic color selected: {dynamic_color}")
    if wom_group_id == 0:
        try:
            wom_group_id = group.wom_id
        except Exception as e:
            print(f"Error getting wom_group_id: {e}")
            wom_group_id = 0
    #f"Group ID: {group_id}")
    if group_id != 2:
        # Fetch player WOM IDs and associated Player IDs
            player_wom_ids = await fetch_group_members(wom_group_id, session_to_use=session)
    else:
        #print("Group ID is 2...")
        player_wom_ids = []
        all_players = session.query(Player.wom_id).all()
        #print(f"Got all players: {len(all_players)}")
        for p in all_players:
            player_wom_ids.append(p.wom_id)
    player_ids = await associate_player_ids(player_wom_ids, session_to_use=session)
    ignored_players_existing = session.query(IgnoredPlayer).filter(IgnoredPlayer.group_id == group_id).all()
    if ignored_players_existing:
        ignored_players = [player.player_id for player in ignored_players_existing]
    else:
        ignored_players = []
    if len(ignored_players) > 0:
        #print(f"Excluding {len(ignored_players)} ignored players from the lootboard generation for group {group_id}")
        player_ids = [player_id for player_id in player_ids if player_id not in ignored_players]
    #print("Got a total of ", len(player_ids), "players for group ", group_id)
    # Get the drops, recent drops, and total loot for the group
    #print("Processed player_ids")
    # Respect group config: optionally filter items and totals by minimum value
    only_include_items_over_minimum_val = config.get('only_include_items_over_minimum', False) if 'config' in locals() else False
    only_include_items_over_minimum_flag = str(only_include_items_over_minimum_val).lower() in ("1", "true", "yes", "on")
    try:
        group_min_value_cfg = int(config.get('minimum_value_to_notify', 500000)) if 'config' in locals() else 500000
    except Exception:
        group_min_value_cfg = 500000
    group_items, player_totals, recent_drops, total_loot = await get_drops_for_group(
        player_ids,
        partition,
        only_include_items_over_minimum=only_include_items_over_minimum_flag,
        group_minimum_value=group_min_value_cfg
    )
    #print("Got recent drops:", len(recent_drops))
    redis_client.client.zadd(f'gleaderboard:{partition}', {group.group_id: total_loot})
    with open(f"/store/droptracker/disc/static/assets/img/clans/{group_id}/recent_drops.json", "w") as f:
        json.dump(recent_drops, f, indent=4)
    # Draw elements on the background image (added dynamic_coloring - added BY Smoke [https://github.com/Varietyz/])
    bg_img = await draw_drops_on_image(bg_img, draw, group_items, group_id, dynamic_colors=use_dynamic_colors, use_gp=use_gp_colors)  # Pass `group_items` here
    bg_img = await draw_headers(group_id, total_loot, bg_img, draw, partition, dynamic_colors=use_dynamic_colors, use_gp=use_gp_colors)  # Draw headers
    bg_img = await draw_recent_drops(bg_img, draw, recent_drops, min_value=minimum_value, dynamic_colors=use_dynamic_colors, use_gp=use_gp_colors)  # Draw recent drops, with a minimum value
    bg_img = await draw_leaderboard(bg_img, draw, player_totals, dynamic_colors=use_dynamic_colors, use_gp=use_gp_colors)  # Draw leaderboard
    save_path = save_image(bg_img, group_id, partition)  # Save the generated image
    #print("Saved the new image.")
    
    # When saving the image, use a different naming convention for daily partitions
    if is_daily:
        # For daily partitions, use the date directly
        file_path = f"/store/droptracker/disc/static/assets/img/clans/{group_id}/lb/daily_{partition.replace('-', '')}.png"
    else:
        # For monthly partitions, use the existing format
        current_date = datetime.now()
        ydmpart = int(current_date.strftime('%d%m%Y'))
        file_path = f"/store/droptracker/disc/static/assets/img/lootboard_temp.png"
    
    return save_path



def get_year_month_string():
    return datetime.now().strftime('%Y-%m')


async def draw_headers(group_id, total_loot, bg_img, draw, partition=None, *, dynamic_colors, use_gp):
    """
    Draw headers on the image, including the title and total loot value.
    The total loot value is displayed using a dynamic color based on its numeric value.
    """
    # Determine if we're using a daily partition.
    is_daily = partition and '-' in str(partition)
    if is_daily:
        try:
            date_obj = datetime.strptime(partition, '%Y-%m-%d')
            date_display = date_obj.strftime('%B %d, %Y')
        except Exception:
            date_display = partition
    else:
        current_month = datetime.now().month
        date_display = calendar.month_name[current_month].capitalize()
    
    # Format total loot for display and compute dynamic color using the numeric value.
    this_month_str = format_number(total_loot)
    value_text_color = get_value_color(total_loot) # (- added BY Smoke [https://github.com/Varietyz/])
    
    # Build the header prefix.
    if int(group_id) == 2:
        prefix = f"Tracked Drops - All Players ({date_display}) - "
    else:
        group = session.query(Group).filter(Group.group_id == group_id).first()
        server_name = group.group_name
        prefix = f"{server_name}'s Tracked Drops for {date_display} - "
    
    # Calculate widths for centering the entire header.
    prefix_bbox = draw.textbbox((0, 0), prefix, font=main_font)
    prefix_width = prefix_bbox[2] - prefix_bbox[0]
    value_bbox = draw.textbbox((0, 0), this_month_str, font=main_font)
    value_width = value_bbox[2] - value_bbox[0]
    total_width = prefix_width + value_width

    bg_img_w, _ = bg_img.size
    head_loc_x = int((bg_img_w - total_width) / 2)
    head_loc_y = 20  # Adjust this if needed.
    if dynamic_colors:
        text_color = get_dynamic_color(bg_img)
    else:
        text_color = yellow
    # Draw the prefix with a fixed text color (e.g. yellow) and a thicker stroke. (color adjustments - added BY Smoke [https://github.com/Varietyz/])
    draw.text((head_loc_x, head_loc_y), prefix, font=main_font,
              fill=text_color, stroke_width=2, stroke_fill=black)
    # Draw the total loot value using the dynamic value_text_color.
    draw.text((head_loc_x + prefix_width, head_loc_y), this_month_str, font=main_font,
              fill=value_text_color, stroke_width=1, stroke_fill=black)
    return bg_img



def load_background_image(filepath):
    bg_img = Image.open(filepath)
    draw = ImageDraw.Draw(bg_img)
    return bg_img, draw


async def draw_leaderboard(bg_img, draw, player_totals, *, dynamic_colors, use_gp, session_to_use = None):
    """
    Draws the leaderboard for players with their total loot values.
    
    :param bg_img: The background image to draw the leaderboard on.
    :param draw: The ImageDraw object used to draw the text.
    :param player_totals: Dictionary of player names and their total loot value.
    :return: Updated background image with the leaderboard drawn on it.
    """
    if session_to_use is not None:
        session = session_to_use
    else:
        session = models.session
    # Sort players by total loot value in descending order, taking the top 12
    top_players = sorted(player_totals.items(), key=lambda x: x[1], reverse=True)[:12]
    
    # Define text positioning
    name_x = 141
    name_y = 228
    pet_font = ImageFont.truetype(rs_font_path, 15)
    first_name = True

    for i, (player, total) in enumerate(top_players):
        # Format player loot totals
        # total_value = int(total)
        total_loot_display = format_number(total)

        # Create rank, name, and loot text
        rank_num_text = f'{i + 1}'
        player_obj = session.query(Player.player_name).filter(Player.player_id == player).first()
        if not player_obj:
            #print("Player with ID", player, " not found.")
            player_rsn = f"Name not found...."
        else:
            player_rsn = player_obj.player_name
        rsn_text = f'{player_rsn}'
        gp_text = f'{total_loot_display}'

        # Determine positions for rank, name, and total loot text
        rank_x, rank_y = (name_x - 104), name_y
        quant_x, quant_y = (name_x + 106), name_y

        # Calculate center for loot (gp_text) and rank_num_text
        quant_bbox = draw.textbbox((0, 0), gp_text, font=pet_font)
        center_q_x = quant_x - (quant_bbox[2] - quant_bbox[0]) / 2

        rsn_bbox = draw.textbbox((0, 0), rsn_text, font=pet_font)
        center_x = name_x - (rsn_bbox[2] - rsn_bbox[0]) / 2

        rank_bbox = draw.textbbox((0, 0), rank_num_text, font=pet_font)
        rank_mid_x = rank_x - (rank_bbox[2] - rank_bbox[0]) / 2

        # Draw text for rank, name, and total loot (colors - added BY Smoke [https://github.com/Varietyz/])
        if dynamic_colors:
            text_color = get_dynamic_color(bg_img)
        else:
            text_color = yellow
        draw.text((center_x, name_y), rsn_text, font=pet_font, fill=text_color, stroke_width=1, stroke_fill=black)
        draw.text((rank_mid_x, rank_y), rank_num_text, font=pet_font, fill=text_color, stroke_width=1, stroke_fill=black)
        draw.text((center_q_x, quant_y), gp_text, font=pet_font, fill=text_color, stroke_width=1, stroke_fill=black)

        # Update Y position for the next player
        if not first_name:
            name_y += 22
        else:
            name_y += 22
            first_name = False

    return bg_img

async def draw_drops_on_image(bg_img, draw, group_items, group_id, *, dynamic_colors=False, use_gp=False):
    """
    Draws the items on the image based on the quantities provided in group_items.
    
    :param bg_img: The background image to draw on.
    :param draw: The ImageDraw object to draw with.
    :param group_items: Dictionary of item_id and corresponding quantities/values.
    :param group_id: The group ID to determine specific placement rules if needed.
    :return: Updated background image with item images and quantities.
    """
    locations = {}
    small_font = ImageFont.truetype(rs_font_path, 16)
    amt_font = ImageFont.truetype(rs_font_path, 18)

    # Load item positions from the CSV file
    with open("data/item-mapping.csv", 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for i, row in enumerate(reader):
            locations[i] = row

    # Sort items by value and limit to top 32
    sorted_items = sorted(group_items.items(), key=lambda x: int(x[1]) if isinstance(x[1], int) else int(x[1].split(',')[1]), reverse=True)[:32]

    for i, (item_id, totals) in enumerate(sorted_items):
        try:
            quantity, total_value = map(int, totals.split(','))
        except ValueError as e:
            #print(f"Error processing item {item_id}: {e}")
            #print(f"Raw data: {totals}")
            continue  # Skip this item and move to the next one

        # print("Item:", sorted_items[i])
        # Get the item's position from the CSV file
        current_pos_x = int(locations[i]['x'])
        current_pos_y = int(locations[i]['y'])
        img_coords = (current_pos_x - 5, current_pos_y - 12)

        # Load the item image based on the item_id ( Coin dynamic loading based on value - added BY Smoke [https://github.com/Varietyz/])
        if int(item_id) == 995:
            coin_img_id = get_coin_image_id(quantity)
            item_img = await load_image_from_id(coin_img_id)
        else:
            item_img = await load_image_from_id(int(item_id))
        if not item_img:
            continue  # Skip if no image found

        # Resize and paste the item image onto the background
        scale_factor = 1.3
        new_width = round(item_img.width * scale_factor)
        new_height = round(item_img.height * scale_factor)
        item_img_resized = item_img.resize((new_width, new_height), Image.Resampling.LANCZOS)
        fixed_img = center_image(item_img_resized, 75, 60)
        bg_img.paste(fixed_img, img_coords, fixed_img)

        value_str = format_number(total_value)
        quantity_str = format_number(quantity)
        ctr_x = current_pos_x + 1
        ctr_y = current_pos_y - 10
        
        if dynamic_colors:
            text_color = get_dynamic_color(bg_img)
        else:
            text_color = yellow
        if use_gp:
            value_text_color = get_value_color(total_value)
        else:
            value_text_color = text_color
        # For coins, since the amount is redundant (equal to the value), display only the value. (- added BY Smoke [https://github.com/Varietyz/])
        if int(item_id) == 995:
            # Use the value-based color instead of the overall dynamic color.
            draw.text((ctr_x, ctr_y + 47), value_str, font=small_font, fill=value_text_color, stroke_width=1, stroke_fill=black)
        else:
            draw.text((ctr_x, ctr_y + 47), value_str, font=small_font, fill=value_text_color, stroke_width=1, stroke_fill=black)
            draw.text((ctr_x, ctr_y + 4), quantity_str, font=amt_font, fill=text_color, stroke_width=1, stroke_fill=black)

    return bg_img


async def draw_recent_drops(bg_img, draw, recent_drops, min_value, *, dynamic_colors, use_gp):
    """
    Draw recent drops on the image, filtering based on a minimum value.
    
    :param bg_img: Background image to draw on.
    :param draw: ImageDraw object to draw elements.
    :param recent_drops: List of recent drops to process.
    :param min_value: The minimum value of drops to be displayed.
    """
    # print("Recent drops:", recent_drops)
    try:
        min_value = int(min_value)
    except TypeError:
        min_value = 2500000
    # Filter the drops based on their value, keeping only those above the specified min_value
    filtered_recents = [drop for drop in recent_drops if drop['value'] >= min_value]
    
    # Drops are already sorted by date from get_drops_for_group_optimized, just limit to 12
    sorted_recents = filtered_recents[:12]
    
    small_font = ImageFont.truetype(rs_font_path, 18)
    recent_locations = {}
    
    # Load locations for placing recent items on the board
    with open("data/recent-mapping.csv", 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for i, row in enumerate(reader):
            recent_locations[i] = row
    
    # Loop through the sorted recent drops and display them
    user_names = {}
    for i, data in enumerate(sorted_recents):
        if "drop_id" not in data:
            continue
        drop = session.query(Drop).filter(Drop.drop_id == data["drop_id"]).first()
        if not drop:
            continue
        player_id = drop.player_id
        player = session.query(Player).filter(Player.player_id == player_id).first()
        if not player:
            user_id = "Unknown"
            user_names[user_id] = "Unknown"
        else:
            user_id = player.player_id
            user_names[user_id] = player.player_name
        # Check if user_id is already cached in the user_names dictionary
        
        username = user_names[user_id]
        date_string = data["date_added"]
        try:
            # Try with microseconds
            date_obj = datetime.strptime(date_string, '%Y-%m-%dT%H:%M:%S.%f')
        except ValueError:
            try:
                # Try ISO format without microseconds
                date_obj = datetime.strptime(date_string, '%Y-%m-%dT%H:%M:%S')
            except ValueError:
                # Fallback to without microseconds and with space
                date_obj = datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S')
        
        # Get the item image based on the item name or ID (Dynamic coins id based on value - added BY Smoke [https://github.com/Varietyz/])
        item_id = data["item_id"]
        if int(item_id) == 995:
            try:
                coin_quantity = int(data["value"])
            except Exception:
                coin_quantity = 1
            coin_img_id = get_coin_image_id(coin_quantity)
            item_img = await load_image_from_id(coin_img_id)
        else:
            item_img = await load_image_from_id(item_id)
        if not item_img:
            continue
        
        # Get the x, y coordinates for the item based on recent_locations
        current_pos_x = int(recent_locations[i]['x'])
        current_pos_y = int(recent_locations[i]['y'])
        
        # Resize and paste the item image onto the background
        scale_factor = 1.3
        new_width = round(item_img.width * scale_factor)
        new_height = round(item_img.height * scale_factor)
        item_img_resized = item_img.resize((new_width, new_height), Image.Resampling.LANCZOS)
        fixed_item_img = center_image(item_img_resized, 75, 60)
        img_coords = (current_pos_x - 5, current_pos_y - 12)
        bg_img.paste(fixed_item_img, img_coords, fixed_item_img)
        
        # Draw text for username and time since the drop
        center_x = (current_pos_x + 1)
        center_y = (current_pos_y - 10)
        current_time = datetime.now()
        time_since = current_time - date_obj
        days, hours, minutes = time_since.days, time_since.seconds // 3600, (time_since.seconds // 60) % 60
        
        if days > 0:
            time_since_disp = f"({days}d {hours}h)"
        elif hours > 0:
            time_since_disp = f"({hours}h {minutes}m)"
        else:
            time_since_disp = f"({minutes}m)"
            
        # coloring (- added BY Smoke [https://github.com/Varietyz/])
        if dynamic_colors:
            text_color = get_dynamic_color(bg_img)
        else:
            text_color = yellow
        draw.text((center_x + 5, center_y), username, font=small_font, fill=text_color, stroke_width=1, stroke_fill=black)
        draw.text((current_pos_x, current_pos_y + 35), time_since_disp, font=small_font, fill=text_color, stroke_width=1, stroke_fill=black)
    return bg_img

def center_image(image, width, height):
    # Create a new image with the desired dimensions and a transparent background
    centered_image = Image.new('RGBA', (width, height), (0, 0, 0, 0))
    # Calculate the position where the original image should be pasted to be centered
    paste_x = (width - image.width) // 2
    paste_y = (height - image.height) // 2
    # Paste the original image onto the new image at the calculated position
    centered_image.paste(image, (paste_x, paste_y))
    return centered_image


def save_image(image, server_id, partition):
    """
    Save the generated lootboard image
    
    Args:
        image: The PIL Image object to save
        server_id: The group/server ID
        partition: The partition string (either YYYYMM or YYYY-MM-DD format)
    """
    base_path = f"/store/droptracker/disc/static/assets/img/clans/{server_id}/lb"
    
    # Determine if this is a daily partition
    is_daily = '-' in str(partition)
    
    if is_daily:
        # For daily partitions (YYYY-MM-DD format)
        date_parts = partition.split('-')
        year = int(date_parts[0])
        month = int(date_parts[1])
        day = int(date_parts[2])
        
        # Get month name (e.g., "August")
        month_name = calendar.month_name[month]
        
        # Create directory structure: /clans/{server_id}/lb/{month_name}/
        month_dir = f"{base_path}/{month_name}"
        os.makedirs(month_dir, exist_ok=True)
        
        # Save as {day}.png in the month directory
        file_path = f"{month_dir}/{day}.png"
        image.save(file_path)
        
        # Also save with year for uniqueness if needed (e.g., {year}_{day}.png)
        # This helps distinguish between same days in different years
        year_file_path = f"{month_dir}/{year}_{day}.png"
        image.save(year_file_path)
        
        # Check if this is today's date
        current_date = datetime.now().strftime('%Y-%m-%d')
        if partition == current_date:
            # Also save as the default lootboard.png if it's today
            os.makedirs(base_path, exist_ok=True)
            image.save(f"{base_path}/lootboard.png")
        
        return file_path
    else:
        # For monthly partitions (YYYYMM format)
        year = int(str(partition)[:4])
        month = int(str(partition)[4:6])
        
        # Get month name
        month_name = calendar.month_name[month]
        
        # Create directory structure
        month_dir = f"{base_path}/{month_name}"
        os.makedirs(month_dir, exist_ok=True)
        
        # Save as monthly summary (e.g., "monthly.png" or "{year}_monthly.png")
        file_path = f"{month_dir}/{year}_monthly.png"
        image.save(file_path)
        
        # Also save as just "monthly.png" for the current view
        monthly_path = f"{month_dir}/monthly.png"
        image.save(monthly_path)
        
        # Check if this is the current month
        current_date = datetime.now()
        current_month_partition = current_date.year * 100 + current_date.month
        if int(partition) == current_month_partition:
            # Save as the default lootboard.png
            os.makedirs(base_path, exist_ok=True)
            ## create a DD-MM-YYYY.png format
            current_date_str = current_date.strftime('%d-%m-%Y')
            image.save(f"{base_path}/lootboard.png")
            image.save(f"{base_path}/{current_date_str}.png")
        
        return file_path


async def load_image_from_id(item_id):
    if item_id == "None" or item_id is None or not isinstance(item_id, int):
        return None
    file_path = f"/store/droptracker/disc/static/assets/img/itemdb/{item_id}.png"
    item = session.query(ItemList).filter(ItemList.item_id == item_id).first()
    item_name = item.item_name
    if item.stackable:
        all_items = session.query(ItemList).filter(ItemList.item_name == item_name).all()
        target_item_id = [max(item.stacked, item.item_id) for item in all_items]
        item_id = target_item_id
    if not os.path.exists(file_path):
        try:
            image_path = await load_rl_cache_img(item_id)
            if image_path:
                file_path = image_path
        except Exception as e:
            print(f"Error loading image for item ID {item_id}: {e}")
    loop = asyncio.get_event_loop()
    try:
        # Run the blocking Image.open operation in a thread pool
        image = await loop.run_in_executor(None, Image.open, file_path)
        return image
    except Exception as e:
        print(f"The following file path: {file_path} produced an error: {e}")
        return None


async def load_rl_cache_img(item_id):
    url = f"https://static.runelite.net/cache/item/icon/{item_id}.png"
    print("Attempting to download a new image for item ID", item_id)
    try:
        ## save it here
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                # Ensure the request was successful
                if response.status != 200:
                    print(f"Failed to fetch image for item ID {item_id}. HTTP status: {response.status}")
                    return None

                # Read the response content
                image_data = await response.read()

                # Load the image data into a PIL Image object
                image = Image.open(BytesIO(image_data))
                file_path = f"/store/droptracker/disc/static/assets/img/itemdb/{item_id}.png"
                # print("Saving")
                image.save(file_path, "PNG")
                print(f"Saved image to {file_path}")
                return file_path

    except Exception as e:
        print("Unable to load the item.")
    finally:
        await aiohttp.ClientSession().close()


def get_hourly_partitions_from_day(year, month, day):
    ## A partition for a day would contain all 24 hours:
    ## Using March 27th, 2025 as an example:
    ## 2025032700 - 2025032723
    ## And also include the last 59 minutes of the day:
    ## 202503272301 - 202503272359
    partitions = []
    for hour in range(24):
        partitions.append(f"{year}{month}{day}{hour:02d}")
        if hour == 23:  
            for minute in range(60):
                if minute == 0:
                    continue
                partitions.append(f"{year}{month}{day}{hour:02d}{minute:02d}")
    return partitions


async def generate_timeframe_board(bot: interactions.Client, group_id: int = 0, wom_group_id: int = 0, 
                                  start_time: datetime = None, end_time: datetime = None, npc_id: int = None):
    """
    Generate a loot board for a specific timeframe and optionally for a specific NPC.
    
    :param bot: Instance of the interactions.Client bot object
    :param group_id: DropTracker GroupID. 0 expects a wom_group_id
    :param wom_group_id: WiseOldMan groupID. 0 expects a group_id
    :param start_time: Start datetime for the timeframe (inclusive)
    :param end_time: End datetime for the timeframe (inclusive)
    :param npc_id: Optional NPC ID to filter drops by
    :return: Path to the generated image
    """
    # Set default times if not provided
    if group_id == 1:
        group_id = 2
    if start_time is None:
        # Default to start of current day
        start_time = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    
    if end_time is None:
        # Default to current time
        end_time = datetime.now()
    
    # Determine the appropriate time granularity based on the timeframe
    time_diff = end_time - start_time
    if time_diff.days > 30:
        # For timeframes longer than a month, use monthly partitions
        granularity = "monthly"
    elif time_diff.days > 1:
        # For timeframes longer than a day, use daily partitions
        granularity = "daily"
    elif time_diff.seconds > 3600:
        # For timeframes longer than an hour, use hourly partitions
        granularity = "hourly"
    else:
        # For shorter timeframes, use minute partitions
        granularity = "minute"
    
    # Get group information
    group = None
    if group_id != 0:  # We prioritize group_id here
        group = session.query(Group).filter(Group.group_id == group_id).first()
    elif wom_group_id != 0:
        group = session.query(Group).filter(Group.wom_id == wom_group_id).first()
    if (group_id != 0 or wom_group_id != 0) and not group:
        print("Cannot generate a lootboard, no group data was properly parsed..")
        return None
    elif (group_id == 0 and wom_group_id == 0):
        group_id = 1
    else:
        group_id = group.group_id
    
    # Get group configuration
    group_config = session.query(GroupConfiguration).filter(GroupConfiguration.group_id == group_id).all()
    config = {conf.config_key: conf.config_value for conf in group_config}
    
    # Get lootboard style and minimum value
    loot_board_style = int(config.get('loot_board_type', 1))
    minimum_value = int(config.get('minimum_value_to_notify', 2500000))
    
    # Load background image
    target_board = session.query(LootboardStyle).filter(LootboardStyle.id == loot_board_style).first()
    local_url = target_board.local_url if target_board else "/store/droptracker/disc/lootboard/themes/bank-new-clean-dark.png"
    bg_img, draw = load_background_image(local_url)
    
    # Get dynamic color settings
    use_dynamic_colors = config.get('use_dynamic_lootboard_colors', True)
    if use_dynamic_colors and use_dynamic_colors == "1":
        use_dynamic_colors = True
    else:
        use_dynamic_colors = False
    use_gp_colors = config.get('use_gp_colors', True)
    
    # Get player IDs for the group
    if group_id != 2:
        if wom_group_id == 0 and group:
            wom_group_id = group.wom_id
        
        if wom_group_id != 0:
            player_wom_ids = await fetch_group_members(wom_group_id)
        else:
            player_wom_ids = [player[0] for player in session.query(Player.wom_id).all()]
    else:
        player_wom_ids = [p.wom_id for p in session.query(Player.wom_id).all()]
    
    player_ids = await associate_player_ids(player_wom_ids)
    
    # Generate time partitions to query
    time_partitions = generate_time_partitions(start_time, end_time, granularity)
    print("Got", len(time_partitions), "time partitions")
    # Get the drops, recent drops, and total loot for the group across all partitions
    group_items, player_totals, recent_drops, total_loot = await get_drops_for_timeframe(
        player_ids, time_partitions, granularity, npc_id
    )

    print("Got recent drops:", len(recent_drops))
    with open(f"/store/droptracker/disc/static/assets/img/clans/{group_id}/recent_drops.json", "w") as f:
        json.dump(recent_drops, f)
    
    # Draw elements on the background image
    bg_img = await draw_drops_on_image(bg_img, draw, group_items, group_id, dynamic_colors=use_dynamic_colors, use_gp=use_gp_colors)
    
    # Create a timeframe string for the header
    if npc_id:
        npc = session.query(NpcList).filter(NpcList.npc_id == npc_id).first()
        npc_name = npc.npc_name if npc else f"Unknown NPC ({npc_id})"
        timeframe_str = f"{start_time.strftime('%Y-%m-%d %H:%M')} to {end_time.strftime('%Y-%m-%d %H:%M')} - {npc_name}"
    else:
        timeframe_str = f"{start_time.strftime('%Y-%m-%d %H:%M')} to {end_time.strftime('%Y-%m-%d %H:%M')}"
    
    # Draw headers with custom timeframe string
    bg_img = await draw_headers(group_id, total_loot, bg_img, draw, timeframe_str, 
                               dynamic_colors=use_dynamic_colors, use_gp=use_gp_colors)
    
    # Draw recent drops and leaderboard
    bg_img = await draw_recent_drops(bg_img, draw, recent_drops, min_value=minimum_value, 
                                    dynamic_colors=use_dynamic_colors, use_gp=use_gp_colors)
    bg_img = await draw_leaderboard(bg_img, draw, player_totals, dynamic_colors=use_dynamic_colors, use_gp=use_gp_colors)
    
    # Save the image with a custom filename
    timeframe_id = f"{start_time.strftime('%Y%m%d%H%M')}-{end_time.strftime('%Y%m%d%H%M')}"
    if npc_id:
        timeframe_id += f"-npc{npc_id}"
    
    image_path = save_image(bg_img, group_id, timeframe_id)
    return image_path

def generate_time_partitions(start_time, end_time, granularity):
    """
    Generate a list of time partition strings between start_time and end_time
    based on the specified granularity.
    """
    partitions = []
    current_time = start_time
    
    if granularity == "monthly":
        # Generate monthly partitions (YYYYMM)
        while current_time <= end_time:
            partition = current_time.strftime('%Y%m')
            if partition not in partitions:
                partitions.append(partition)
            # Move to next month
            if current_time.month == 12:
                current_time = current_time.replace(year=current_time.year + 1, month=1)
            else:
                current_time = current_time.replace(month=current_time.month + 1)
    
    elif granularity == "daily":
        # Generate daily partitions (YYYYMMDD)
        while current_time <= end_time:
            partition = current_time.strftime('%Y%m%d')
            if partition not in partitions:
                partitions.append(partition)
            # Move to next day
            current_time = current_time + timedelta(days=1)
    
    elif granularity == "hourly":
        # Generate hourly partitions (YYYYMMDDHH)
        while current_time <= end_time:
            partition = current_time.strftime('%Y%m%d%H')
            if partition not in partitions:
                partitions.append(partition)
            # Move to next hour
            current_time = current_time + timedelta(hours=1)
    
    else:  # minute
        # Generate minute partitions (YYYYMMDDHHMM)
        while current_time <= end_time:
            partition = current_time.strftime('%Y%m%d%H%M')
            if partition not in partitions:
                partitions.append(partition)
            # Move to next minute
            current_time = current_time + timedelta(minutes=1)
    
    return partitions

async def get_drops_for_timeframe(player_ids, time_partitions, granularity, npc_id=None):
    """
    Returns the drops stored in redis cache for the specific list of player_ids
    across multiple time partitions, optionally filtered by NPC.
    """
    group_items = {}
    recent_drops = []
    total_loot = 0
    player_totals = {}
    
    # Determine Redis key prefix based on granularity
    if granularity == "monthly":
        prefix = ""  # Monthly has no prefix
    elif granularity == "daily":
        prefix = "daily"
    elif granularity == "hourly":
        prefix = "hourly"
    else:
        prefix = "minute"
    
    async def process_player(player_id):
        nonlocal total_loot
        player_total = 0
        for partition in time_partitions:
            # Construct Redis keys based on granularity
            if prefix:
                total_items_key = f"player:{player_id}:{prefix}:{partition}:items"
                recent_items_key = f"player:{player_id}:{prefix}:{partition}:recent_items"
                loot_key = f"player:{player_id}:{prefix}:{partition}:total_loot"
                
                # If filtering by NPC, use the NPC-specific keys
                if npc_id:
                    total_items_key = f"player:{player_id}:{prefix}:{partition}:npc_items:{npc_id}"
            else:
                # Monthly format
                total_items_key = f"player:{player_id}:{partition}:total_items"
                recent_items_key = f"player:{player_id}:{partition}:recent_items"
                loot_key = f"player:{player_id}:{partition}:total_loot"
                
                # If filtering by NPC, we need to check NPC totals
                if npc_id:
                    npc_key = f"player:{player_id}:{partition}:npc_totals"
                    npc_value = redis_client.client.hget(npc_key, str(npc_id))
                    if npc_value:
                        player_total += int(npc_value.decode('utf-8'))
                    continue  # Skip item processing for monthly NPC filtering
            
            # Get total items
            total_items = redis_client.client.hgetall(total_items_key)
            
            for key, value in total_items.items():
                key = key.decode('utf-8')
                value = value.decode('utf-8')
                try:
                    quantity, total_value = map(int, value.split(','))
                except ValueError:
                    continue
                
                if key in group_items:
                    existing_quantity, existing_value = map(int, group_items[key].split(','))
                    new_quantity = existing_quantity + quantity
                    new_total_value = existing_value + total_value
                    group_items[key] = f"{new_quantity},{new_total_value}"
                else:
                    group_items[key] = f"{quantity},{total_value}"
            
            # Get player total for this partition
            if not npc_id or prefix:  # Skip for monthly NPC filtering
                partition_total = redis_client.client.get(loot_key)
                if partition_total:
                    partition_total = int(partition_total.decode('utf-8'))
                    if npc_id and prefix:
                        # For time-based NPC filtering, we need to check if this total is for our NPC
                        npc_key = f"player:{player_id}:{prefix}:{partition}:npcs"
                        npc_value = redis_client.client.hget(npc_key, str(npc_id))
                        if npc_value:
                            player_total += int(npc_value.decode('utf-8'))
                    else:
                        player_total += partition_total
            
            # Get recent items
            if not npc_id:  # Only process recent items if not filtering by NPC
                recent_items_raw = redis_client.client.lrange(recent_items_key, 0, -1)
                for item in recent_items_raw:
                    item_data = json.loads(item.decode('utf-8'))
                    # Add to recent drops if not already present
                    if not any(drop['drop_id'] == item_data['drop_id'] for drop in recent_drops):
                        recent_drops.append(item_data)
                        
        if player_total > 0:
            print(f"Player {player_id} total:", player_total)
        return player_id, player_total, []  # Empty list for recent items as we process them separately
    
    # Process all players concurrently
    tasks = [process_player(player_id) for player_id in player_ids]
    results = await asyncio.gather(*tasks)
    
    # Process results
    for player_id, player_total, _ in results:
        if player_total > 0:
            player_totals[player_id] = player_total
            total_loot += player_total
    
    # Sort recent drops by date (newest first)
    recent_drops.sort(key=lambda x: x['date_added'], reverse=True)
    
    # Limit to most recent 10 drops
    recent_drops = recent_drops[:10]
    
    return group_items, player_totals, recent_drops, total_loot
