"""
Flexible Lootboard Generator

This module provides a flexible board generation system that can create lootboards
with various filters and contexts including:
- Date/time range filtering
- Player ID filtering
- NPC-specific filtering
- Custom partition handling
"""

import asyncio
import csv
import calendar
import json
import os
import time
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Set, Union
from dataclasses import dataclass
from enum import Enum

from sqlalchemy import and_, select
from db.models import Drop, Guild, IgnoredPlayer, LootboardStyle, Player, ItemList, Session, session, Group, GroupConfiguration, NpcList
from db import models
from io import BytesIO

import aiohttp
from PIL import Image, ImageFont, ImageDraw

from utils.redis import RedisClient
from utils.wiseoldman import fetch_group_members
from db.ops import DatabaseOperations, associate_player_ids

from utils.format import format_number
from utils.dynamic_handling import get_value_color, get_dynamic_color, get_coin_image_id

# Import existing generator functions for reuse
from lootboard.generator import (
    load_background_image, draw_headers, draw_drops_on_image, 
    draw_recent_drops, draw_leaderboard, center_image, 
    load_image_from_id, save_image, get_db_session
)

redis_client = RedisClient()
db = DatabaseOperations()

class FilterMode(Enum):
    """Filtering modes for board generation"""
    INCLUDE = "include"  # Include only matching items
    EXCLUDE = "exclude"  # Exclude matching items

class TimeGranularity(Enum):
    """Time granularity for partition generation"""
    MONTHLY = "monthly"    # YYYYMM (actual Redis structure)
    ALL_TIME = "all_time"  # all (actual Redis structure)
    DAILY = "daily"        # YYYYMMDD (new daily Redis structure)

@dataclass
class BoardFilter:
    """Comprehensive filter configuration for board generation"""
    # Time filtering
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    time_granularity: TimeGranularity = TimeGranularity.MONTHLY
    
    # Player filtering
    player_ids: Optional[List[int]] = None
    player_filter_mode: FilterMode = FilterMode.INCLUDE
    
    # NPC filtering
    npc_ids: Optional[List[int]] = None
    npc_filter_mode: FilterMode = FilterMode.INCLUDE
    
    # Item filtering
    item_ids: Optional[List[int]] = None
    item_filter_mode: FilterMode = FilterMode.INCLUDE
    min_item_value: Optional[int] = None
    max_item_value: Optional[int] = None
    
    # Drop filtering
    min_drop_value: Optional[int] = None
    max_drop_value: Optional[int] = None
    min_quantity: Optional[int] = None
    max_quantity: Optional[int] = None
    
    # Partition filtering
    partitions: Optional[List[str]] = None

@dataclass
class BoardData:
    """Aggregated data for board generation"""
    group_items: Dict[str, str]  # item_id -> "quantity,total_value"
    player_totals: Dict[int, int]  # player_id -> total_value
    recent_drops: List[Dict]
    total_loot: int
    metadata: Dict = None

class FlexibleBoardGenerator:
    """
    Advanced board generator with comprehensive filtering capabilities
    """
    
    def __init__(self):
        self.redis_client = redis_client
        
    def _get_partition_from_datetime(self, dt: datetime, granularity: TimeGranularity) -> str:
        """Generate partition string from datetime based on granularity"""
        if granularity == TimeGranularity.MONTHLY:
            return str(dt.year * 100 + dt.month)  # Match redis_updates.py format
        elif granularity == TimeGranularity.ALL_TIME:
            return "all"  # Match redis_updates.py format
        elif granularity == TimeGranularity.DAILY:
            return dt.strftime('%Y%m%d')  # YYYYMMDD format
        else:
            return str(dt.year * 100 + dt.month)  # Default to monthly
    
    def _generate_time_partitions(self, start_time: datetime, end_time: datetime, 
                                 granularity: TimeGranularity) -> List[str]:
        """Generate list of partition strings between start and end times"""
        partitions = set()
        current_time = start_time
        
        if granularity == TimeGranularity.MONTHLY:
            while current_time <= end_time:
                partitions.add(str(current_time.year * 100 + current_time.month))
                # Move to next month
                if current_time.month == 12:
                    current_time = current_time.replace(year=current_time.year + 1, month=1)
                else:
                    current_time = current_time.replace(month=current_time.month + 1)
                    
        elif granularity == TimeGranularity.DAILY:
            while current_time <= end_time:
                partitions.add(current_time.strftime('%Y%m%d'))
                current_time += timedelta(days=1)
                    
        elif granularity == TimeGranularity.ALL_TIME:
            # All-time only has one partition
            partitions.add("all")
        
        return sorted(list(partitions))
    
    def _get_redis_keys_for_partition(self, player_id: int, partition: str, 
                                    granularity: TimeGranularity) -> Dict[str, str]:
        """Generate Redis keys for a player and partition - matches redis_updates.py structure"""
        if granularity == TimeGranularity.DAILY:
            # Daily keys use the daily: prefix
            return {
                'total_items': f"player:{player_id}:daily:{partition}:total_items",
                'total_loot': f"player:{player_id}:daily:{partition}:total_loot",
                'recent_items': f"player:{player_id}:daily:{partition}:recent_items",
                'drop_history': f"player:{player_id}:daily:{partition}:drop_history",
                'high_value_items': f"player:{player_id}:daily:{partition}:high_value_items"
            }
        else:
            # Monthly and all-time use the same key pattern from redis_updates.py
            return {
                'total_items': f"player:{player_id}:{partition}:total_items",
                'total_loot': f"player:{player_id}:{partition}:total_loot",
                'recent_items': f"player:{player_id}:{partition}:recent_items",
                'drop_history': f"player:{player_id}:{partition}:drop_history",
                'high_value_items': f"player:{player_id}:{partition}:high_value_items"
            }
    
    async def _get_player_data_for_partition(self, player_id: int, partition: str, 
                                           granularity: TimeGranularity, 
                                           board_filter: BoardFilter) -> Tuple[Dict, int, List]:
        """Get player data for a specific partition with filtering"""
        keys = self._get_redis_keys_for_partition(player_id, partition, granularity)
        
        player_items = {}
        player_total = 0
        player_recent_drops = []
        
        # Get total loot for this partition
        total_loot_raw = self.redis_client.client.get(keys['total_loot'])
        if total_loot_raw:
            player_total = int(total_loot_raw.decode('utf-8'))
        
        # Get items data
        items_data = self.redis_client.client.hgetall(keys['total_items'])
        
        for item_id_bytes, item_data_bytes in items_data.items():
            item_id = int(item_id_bytes.decode('utf-8'))
            item_data = item_data_bytes.decode('utf-8')
            
            # Apply item ID filtering
            if board_filter.item_ids:
                if board_filter.item_filter_mode == FilterMode.INCLUDE:
                    if item_id not in board_filter.item_ids:
                        continue
                elif board_filter.item_filter_mode == FilterMode.EXCLUDE:
                    if item_id in board_filter.item_ids:
                        continue
            
            try:
                # Parse item data: "quantity,total_value,drop_count,first_drop,last_drop"
                parts = item_data.split(',')
                if len(parts) >= 2:
                    quantity = int(parts[0])
                    total_value = int(parts[1])
                    
                    # Apply value filtering
                    if board_filter.min_item_value and total_value < board_filter.min_item_value:
                        continue
                    if board_filter.max_item_value and total_value > board_filter.max_item_value:
                        continue
                    
                    # Apply quantity filtering
                    if board_filter.min_quantity and quantity < board_filter.min_quantity:
                        continue
                    if board_filter.max_quantity and quantity > board_filter.max_quantity:
                        continue
                    
                    player_items[str(item_id)] = f"{quantity},{total_value}"
                    
            except (ValueError, IndexError):
                continue
        
        # Get recent items with filtering
        recent_items_raw = self.redis_client.client.lrange(keys['recent_items'], 0, -1)
        
        for item_raw in recent_items_raw:
            try:
                item_data = json.loads(item_raw.decode('utf-8'))
                
                # Apply time filtering for recent items
                if board_filter.start_time or board_filter.end_time:
                    try:
                        item_date = datetime.fromisoformat(item_data['date_added'].replace('Z', '+00:00'))
                    except:
                        try:
                            item_date = datetime.strptime(item_data['date_added'], '%Y-%m-%d %H:%M:%S')
                        except:
                            continue
                    
                    if board_filter.start_time and item_date < board_filter.start_time:
                        continue
                    if board_filter.end_time and item_date > board_filter.end_time:
                        continue
                
                # Apply NPC filtering
                if board_filter.npc_ids and 'npc_id' in item_data:
                    npc_id = item_data['npc_id']
                    if board_filter.npc_filter_mode == FilterMode.INCLUDE:
                        if npc_id not in board_filter.npc_ids:
                            continue
                    elif board_filter.npc_filter_mode == FilterMode.EXCLUDE:
                        if npc_id in board_filter.npc_ids:
                            continue
                
                # Apply drop value filtering
                if 'total_value' in item_data:
                    drop_value = item_data['total_value']
                    if board_filter.min_drop_value and drop_value < board_filter.min_drop_value:
                        continue
                    if board_filter.max_drop_value and drop_value > board_filter.max_drop_value:
                        continue
                
                player_recent_drops.append(item_data)
                
            except (json.JSONDecodeError, KeyError):
                continue
        
        return player_items, player_total, player_recent_drops
    
    async def _aggregate_player_data(self, player_ids: List[int], partitions: List[str], 
                                   granularity: TimeGranularity, 
                                   board_filter: BoardFilter) -> BoardData:
        """Aggregate data across multiple players and partitions"""
        
        group_items = {}
        player_totals = {}
        all_recent_drops = []
        total_loot = 0
        
        # Apply player filtering
        if board_filter.player_ids:
            if type(board_filter.player_ids) == int:
                board_filter.player_ids = [board_filter.player_ids]
            if board_filter.player_filter_mode == FilterMode.INCLUDE:
                player_ids = [pid for pid in player_ids if pid in board_filter.player_ids]
            elif board_filter.player_filter_mode == FilterMode.EXCLUDE:
                player_ids = [pid for pid in player_ids if pid not in board_filter.player_ids]
        
        # Process each player across all partitions
        for player_id in player_ids:
            player_total_across_partitions = 0
            
            for partition in partitions:
                player_items, player_partition_total, player_recent_drops = await self._get_player_data_for_partition(
                    player_id, partition, granularity, board_filter
                )
                
                # Aggregate items
                for item_id, item_data in player_items.items():
                    quantity, total_value = map(int, item_data.split(','))
                    
                    if item_id in group_items:
                        existing_quantity, existing_value = map(int, group_items[item_id].split(','))
                        new_quantity = existing_quantity + quantity
                        new_total_value = existing_value + total_value
                        group_items[item_id] = f"{new_quantity},{new_total_value}"
                    else:
                        group_items[item_id] = item_data
                
                # Aggregate player totals
                player_total_across_partitions += player_partition_total
                
                # Collect recent drops
                all_recent_drops.extend(player_recent_drops)
            
            if player_total_across_partitions > 0:
                player_totals[player_id] = player_total_across_partitions
                total_loot += player_total_across_partitions
        
        # Sort and limit recent drops
        all_recent_drops.sort(key=lambda x: x.get('date_added', ''), reverse=True)
        recent_drops = all_recent_drops[:50]  # Limit to 50 most recent
        
        # Metadata
        metadata = {
            'total_players': len(player_totals),
            'total_partitions': len(partitions),
            'granularity': granularity.value,
            'filter_applied': {
                'time_range': bool(board_filter.start_time or board_filter.end_time),
                'player_filter': bool(board_filter.player_ids),
                'npc_filter': bool(board_filter.npc_ids),
                'item_filter': bool(board_filter.item_ids),
                'value_filter': bool(board_filter.min_item_value or board_filter.max_item_value)
            }
        }
        
        return BoardData(
            group_items=group_items,
            player_totals=player_totals,
            recent_drops=recent_drops,
            total_loot=total_loot,
            metadata=metadata
        )
    
    async def generate_flexible_board(self, group_id: int = 0, wom_group_id: int = 0, 
                                    board_filter: BoardFilter = None, 
                                    session_to_use=None) -> str:
        """
        Generate a flexible board with comprehensive filtering options
        
        Args:
            group_id: DropTracker group ID
            wom_group_id: WiseOldMan group ID  
            board_filter: BoardFilter object with filtering options
            session_to_use: Database session to use
            
        Returns:
            Path to generated board image
        """
        
        if board_filter is None:
            board_filter = BoardFilter()
        
        if session_to_use is not None:
            session = session_to_use
        else:
            session = models.session
        
        # Get group information
        group = None
        if group_id != 0:
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
        
        # Get board style and settings
        loot_board_style = int(config.get('loot_board_type', 1))
        minimum_value = int(config.get('minimum_value_to_notify', 2500000))
        
        # Load background image
        target_board = session.query(LootboardStyle).filter(LootboardStyle.id == loot_board_style).first()
        if not target_board:
            target_board = session.query(LootboardStyle).filter(LootboardStyle.id == 1).first()
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
                player_wom_ids = await fetch_group_members(wom_group_id, session_to_use=session)
            else:
                player_wom_ids = [player[0] for player in session.query(Player.wom_id).all()]
        else:
            player_wom_ids = [p.wom_id for p in session.query(Player.wom_id).all()]
        
        player_ids = await associate_player_ids(player_wom_ids, session_to_use=session)
        
        # Handle ignored players
        ignored_players_existing = session.query(IgnoredPlayer).filter(IgnoredPlayer.group_id == group_id).all()
        if ignored_players_existing:
            ignored_players = [player.player_id for player in ignored_players_existing]
            player_ids = [player_id for player_id in player_ids if player_id not in ignored_players]
        
        # Generate partitions based on time filter
        if board_filter.partitions:
            partitions = board_filter.partitions
        elif board_filter.time_granularity == TimeGranularity.ALL_TIME:
            # Use all-time partition
            partitions = ["all"]
        elif board_filter.start_time or board_filter.end_time:
            start_time = board_filter.start_time or datetime(2020, 1, 1)
            end_time = board_filter.end_time or datetime.now()
            partitions = self._generate_time_partitions(start_time, end_time, board_filter.time_granularity)
        else:
            # Default to current month
            current_date = datetime.now()
            partitions = [str(current_date.year * 100 + current_date.month)]
        
        print(f"Processing {len(player_ids)} players across {len(partitions)} partitions")
        
        # Get aggregated board data
        board_data = await self._aggregate_player_data(
            player_ids, partitions, board_filter.time_granularity, board_filter
        )
        
        print(f"Generated board data: {board_data.total_loot:,} total loot, {len(board_data.recent_drops)} recent drops")
        
        # Update group leaderboard if using current partition
        if len(partitions) == 1 and partitions[0] == datetime.now().strftime('%Y%m'):
            redis_client.client.zadd(f'gleaderboard:{partitions[0]}', {group.group_id: board_data.total_loot})
        
        # Save recent drops
        with open(f"/store/droptracker/disc/static/assets/img/clans/{group_id}/recent_drops_flexible.json", "w") as f:
            json.dump(board_data.recent_drops, f, indent=4)
        
        # Draw elements on the background image
        bg_img = await draw_drops_on_image(bg_img, draw, board_data.group_items, group_id, 
                                         dynamic_colors=use_dynamic_colors, use_gp=use_gp_colors)
        
        # Create custom partition string for header
        partition_display = self._create_partition_display_string(board_filter, partitions)
        
        bg_img = await draw_headers(group_id, board_data.total_loot, bg_img, draw, partition_display,
                                  dynamic_colors=use_dynamic_colors, use_gp=use_gp_colors)
        
        bg_img = await draw_recent_drops(bg_img, draw, board_data.recent_drops, min_value=minimum_value,
                                       dynamic_colors=use_dynamic_colors, use_gp=use_gp_colors)
        
        bg_img = await draw_leaderboard(bg_img, draw, board_data.player_totals,
                                      dynamic_colors=use_dynamic_colors, use_gp=use_gp_colors)
        
        # Save the image with descriptive filename
        filename = self._generate_filename(board_filter, partitions)
        
        # Create custom save path
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        save_path = f"/store/droptracker/disc/static/assets/img/clans/{group_id}/flexible_{filename}_{timestamp}.png"
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        bg_img.save(save_path)
        
        print(f"Saved flexible board to: {save_path}")
        return save_path
    
    def _create_partition_display_string(self, board_filter: BoardFilter, partitions: List[str]) -> str:
        """Create a human-readable display string for the partition(s)"""
        if board_filter.start_time and board_filter.end_time:
            start_str = board_filter.start_time.strftime('%Y-%m-%d %H:%M')
            end_str = board_filter.end_time.strftime('%Y-%m-%d %H:%M')
            return f"{start_str} to {end_str}"
        elif len(partitions) == 1:
            partition = partitions[0]
            if partition == "all":
                return "All Time"
            elif len(partition) == 6:  # YYYYMM
                year = int(partition[:4])
                month = int(partition[4:6])
                return f"{calendar.month_name[month]} {year}"
            elif len(partition) == 8:  # YYYYMMDD
                year = int(partition[:4])
                month = int(partition[4:6])
                day = int(partition[6:8])
                return f"{calendar.month_name[month]} {day}, {year}"
        else:
            return f"Multiple Periods ({len(partitions)} partitions)"
    
    def _generate_filename(self, board_filter: BoardFilter, partitions: List[str]) -> str:
        """Generate a descriptive filename for the board"""
        parts = []
        
        if board_filter.start_time and board_filter.end_time:
            start_str = board_filter.start_time.strftime('%Y%m%d')
            end_str = board_filter.end_time.strftime('%Y%m%d')
            parts.append(f"{start_str}-{end_str}")
        elif len(partitions) == 1:
            parts.append(partitions[0])
        else:
            parts.append(f"multi_{len(partitions)}")
        
        if board_filter.player_ids:
            parts.append(f"players_{len(board_filter.player_ids)}")
        
        if board_filter.npc_ids:
            if len(board_filter.npc_ids) == 1:
                parts.append(f"npc_{board_filter.npc_ids[0]}")
            else:
                parts.append(f"npcs_{len(board_filter.npc_ids)}")
        
        if board_filter.item_ids:
            parts.append(f"items_{len(board_filter.item_ids)}")
        
        if board_filter.min_item_value:
            parts.append(f"minval_{board_filter.min_item_value}")
        
        return "_".join(parts) if parts else "default"


# Convenience functions for common use cases
async def generate_date_range_board(group_id: int, start_date: datetime, end_date: datetime, 
                                  granularity: TimeGranularity = TimeGranularity.MONTHLY) -> str:
    """Generate board for a specific date range"""
    board_filter = BoardFilter(
        start_time=start_date,
        end_time=end_date,
        time_granularity=granularity
    )
    generator = FlexibleBoardGenerator()
    return await generator.generate_flexible_board(group_id=group_id, board_filter=board_filter)

async def generate_player_board(group_id: int, player_ids: List[int], 
                              partition: str = None) -> str:
    """Generate board for specific players"""
    board_filter = BoardFilter(
        player_ids=player_ids,
        partitions=[partition] if partition else None
    )
    generator = FlexibleBoardGenerator()
    return await generator.generate_flexible_board(group_id=group_id, board_filter=board_filter)

async def generate_npc_board(group_id: int, npc_ids: List[int], 
                           start_date: datetime = None, end_date: datetime = None) -> str:
    """Generate board for specific NPCs"""
    # Use daily granularity if date range is provided and spans less than 32 days
    granularity = TimeGranularity.MONTHLY
    if start_date and end_date:
        day_diff = (end_date - start_date).days
        if day_diff <= 31:  # Use daily for month or less
            granularity = TimeGranularity.DAILY
    
    board_filter = BoardFilter(
        npc_ids=npc_ids,
        start_time=start_date,
        end_time=end_date,
        time_granularity=granularity
    )
    generator = FlexibleBoardGenerator()
    return await generator.generate_flexible_board(group_id=group_id, board_filter=board_filter)

async def generate_high_value_board(group_id: int, min_value: int, 
                                  start_date: datetime = None, end_date: datetime = None) -> str:
    """Generate board for high-value items only"""
    board_filter = BoardFilter(
        min_item_value=min_value,
        start_time=start_date,
        end_time=end_date,
        time_granularity=TimeGranularity.MONTHLY  # Only monthly granularity available
    )
    generator = FlexibleBoardGenerator()
    return await generator.generate_flexible_board(group_id=group_id, board_filter=board_filter)

# All-time specific convenience functions
async def generate_all_time_board(group_id: int) -> str:
    """Generate all-time board for a group"""
    board_filter = BoardFilter(
        time_granularity=TimeGranularity.ALL_TIME
    )
    generator = FlexibleBoardGenerator()
    return await generator.generate_flexible_board(group_id=group_id, board_filter=board_filter)

async def generate_all_time_player_board(group_id: int, player_ids: Union[int, List[int]]) -> str:
    """Generate all-time board for specific players"""
    if isinstance(player_ids, int):
        player_ids = [player_ids]
    
    board_filter = BoardFilter(
        player_ids=player_ids,
        time_granularity=TimeGranularity.ALL_TIME
    )
    generator = FlexibleBoardGenerator()
    return await generator.generate_flexible_board(group_id=group_id, board_filter=board_filter)

async def generate_all_time_npc_board(group_id: int, npc_ids: List[int]) -> str:
    """Generate all-time board for specific NPCs"""
    board_filter = BoardFilter(
        npc_ids=npc_ids,
        time_granularity=TimeGranularity.ALL_TIME
    )
    generator = FlexibleBoardGenerator()
    return await generator.generate_flexible_board(group_id=group_id, board_filter=board_filter)

async def generate_all_time_high_value_board(group_id: int, min_value: int) -> str:
    """Generate all-time board for high-value items"""
    board_filter = BoardFilter(
        min_item_value=min_value,
        time_granularity=TimeGranularity.ALL_TIME
    )
    generator = FlexibleBoardGenerator()
    return await generator.generate_flexible_board(group_id=group_id, board_filter=board_filter)

# Daily specific convenience functions
async def generate_daily_board(group_id: int, target_date: datetime = None) -> str:
    """Generate daily board for a specific date (defaults to today)"""
    if target_date is None:
        target_date = datetime.now()
    
    # Set start and end to the same day
    start_date = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = target_date.replace(hour=23, minute=59, second=59, microsecond=999999)
    
    board_filter = BoardFilter(
        start_time=start_date,
        end_time=end_date,
        time_granularity=TimeGranularity.DAILY
    )
    generator = FlexibleBoardGenerator()
    return await generator.generate_flexible_board(group_id=group_id, board_filter=board_filter)

async def generate_daily_player_board(group_id: int, player_ids: Union[int, List[int]], 
                                    target_date: datetime = None) -> str:
    """Generate daily board for specific players"""
    if isinstance(player_ids, int):
        player_ids = [player_ids]
    
    if target_date is None:
        target_date = datetime.now()
    
    start_date = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = target_date.replace(hour=23, minute=59, second=59, microsecond=999999)
    
    board_filter = BoardFilter(
        player_ids=player_ids,
        start_time=start_date,
        end_time=end_date,
        time_granularity=TimeGranularity.DAILY
    )
    generator = FlexibleBoardGenerator()
    return await generator.generate_flexible_board(group_id=group_id, board_filter=board_filter)

async def generate_daily_npc_board(group_id: int, npc_ids: List[int], 
                                 target_date: datetime = None) -> str:
    """Generate daily board for specific NPCs"""
    if target_date is None:
        target_date = datetime.now()
    
    start_date = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
    end_date = target_date.replace(hour=23, minute=59, second=59, microsecond=999999)
    
    board_filter = BoardFilter(
        npc_ids=npc_ids,
        start_time=start_date,
        end_time=end_date,
        time_granularity=TimeGranularity.DAILY
    )
    generator = FlexibleBoardGenerator()
    return await generator.generate_flexible_board(group_id=group_id, board_filter=board_filter)

async def generate_date_range_daily_board(group_id: int, start_date: datetime, 
                                        end_date: datetime) -> str:
    """Generate board for a date range using daily granularity"""
    board_filter = BoardFilter(
        start_time=start_date,
        end_time=end_date,
        time_granularity=TimeGranularity.DAILY
    )
    generator = FlexibleBoardGenerator()
    return await generator.generate_flexible_board(group_id=group_id, board_filter=board_filter)
