### Contains the new functional implementation for interacting with the redis dicts for players' drops
## As it relates to generating new loot leaderboards
from db import Drop, Player, session, Group, models
from utils.format import format_number
from utils.redis import redis_client
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple, Set
import json
import threading
import time
from dataclasses import dataclass
from enum import Enum   
from sqlalchemy.orm import joinedload
from utils.format import get_current_partition

class UpdateMode(Enum):
    INCREMENTAL = "incremental"  # Add new drops to existing data
    FORCE_UPDATE = "force_update"  # Recalculate everything from database

@dataclass
class LootLeaderboardQuery:
    """Query parameters for generating loot leaderboards"""
    player_ids: Optional[List[int]] = None
    npc_ids: Optional[List[int]] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    min_item_value: Optional[int] = None  # For high-value item tracking
    partition: Optional[int] = None  # Monthly partition (YYYYMM)

@dataclass
class PlayerItemData:
    """Individual item data for a player"""
    item_id: int
    quantity: int
    total_value: int
    drop_count: int
    first_drop: datetime
    last_drop: datetime

@dataclass
class PlayerLootSummary:
    """Complete loot summary for a player"""
    player_id: int
    total_value: int
    total_drops: int
    unique_items: int
    unique_npcs: int
    items: Dict[int, PlayerItemData]
    high_value_items: List[Dict]  # Items exceeding min_item_value threshold

class RedisLootTracker:
    """
    Functional implementation for Redis-based loot tracking and leaderboard generation.
    Handles both incremental updates and force updates with concurrency safety.
    """
    
    def __init__(self):
        self._lock = threading.RLock()  # Reentrant lock for thread safety
        self._processing_players: Set[int] = set()  # Track players being processed
        
    def _get_partition(self, dt: datetime = None) -> int:
        return get_current_partition()  
    
    def _get_redis_keys(self, player_id: int, partition: int, drop_date: datetime = None) -> Dict[str, str]:
        """Generate Redis keys for a player and partition"""
        base_keys = {
            'total_items': f"player:{player_id}:{partition}:total_items",
            'total_loot': f"player:{player_id}:{partition}:total_loot",
            'recent_items': f"player:{player_id}:{partition}:recent_items",
            'drop_history': f"player:{player_id}:{partition}:drop_history",
            'high_value_items': f"player:{player_id}:{partition}:high_value_items",
            'all_time_total_items': f"player:{player_id}:all:total_items",
            'all_time_total_loot': f"player:{player_id}:all:total_loot",
            'all_time_recent_items': f"player:{player_id}:all:recent_items",
            'all_time_high_value_items': f"player:{player_id}:all:high_value_items"
        }
        
        # Add daily keys if drop_date is provided
        if drop_date:
            daily_partition = drop_date.strftime('%Y%m%d')  # YYYYMMDD format
            base_keys.update({
                'daily_total_items': f"player:{player_id}:daily:{daily_partition}:total_items",
                'daily_total_loot': f"player:{player_id}:daily:{daily_partition}:total_loot",
                'daily_recent_items': f"player:{player_id}:daily:{daily_partition}:recent_items",
                'daily_drop_history': f"player:{player_id}:daily:{daily_partition}:drop_history",
                'daily_high_value_items': f"player:{player_id}:daily:{daily_partition}:high_value_items"
            })
        
        return base_keys
    
    def _atomic_hash_update_script(self) -> str:
        """Lua script for atomic hash updates"""
        return """
        local key = KEYS[1]
        local item_id = ARGV[1]
        local qty_delta = tonumber(ARGV[2])
        local value_delta = tonumber(ARGV[3])
        local force_update = ARGV[4] == "true"
        local drop_count_delta = tonumber(ARGV[5])
        local first_drop = ARGV[6]
        local last_drop = ARGV[7]
        
        local current = redis.call('HGET', key, item_id)
        local new_qty, new_value, new_drop_count, new_first_drop, new_last_drop
        
        if current and not force_update then
            local parts = {}
            for part in string.gmatch(current, "[^,]+") do
                table.insert(parts, part)
            end
            
            if #parts >= 5 then
                local existing_qty = tonumber(parts[1])
                local existing_value = tonumber(parts[2])
                local existing_drop_count = tonumber(parts[3])
                local existing_first_drop = parts[4]
                local existing_last_drop = parts[5]
                
                new_qty = existing_qty + qty_delta
                new_value = existing_value + value_delta
                new_drop_count = existing_drop_count + drop_count_delta
                new_first_drop = existing_first_drop
                new_last_drop = last_drop  -- Always update to latest
            else
                new_qty = qty_delta
                new_value = value_delta
                new_drop_count = drop_count_delta
                new_first_drop = first_drop
                new_last_drop = last_drop
            end
        else
            new_qty = qty_delta
            new_value = value_delta
            new_drop_count = drop_count_delta
            new_first_drop = first_drop
            new_last_drop = last_drop
        end
        
        local result = new_qty .. "," .. new_value .. "," .. new_drop_count .. "," .. new_first_drop .. "," .. new_last_drop
        redis.call('HSET', key, item_id, result)
        return result
        """
    
    def add_to_player(self, player: Player, drop: Drop) -> bool:
        """
        Add a single drop to a player's Redis cache (incremental update).
        Thread-safe and atomic.
        """
        with self._lock:
            if player.player_id in self._processing_players:
                # Player is being force-updated, skip incremental update
                return False
            
            try:
                return self._add_drop_incremental(player.player_id, drop)
            except Exception as e:
                print(f"Error adding drop {drop.drop_id} to player {player.player_id}: {e}")
                return False
    
    def _add_drop_incremental(self, player_id: int, drop: Drop) -> bool:
        """Internal method for incremental drop addition"""
        partition = self._get_partition(drop.date_added)
        keys = self._get_redis_keys(player_id, partition, drop.date_added)  # Pass drop_date for daily keys
        
        # Calculate drop values
        total_value = drop.value * drop.quantity
        drop_timestamp = drop.date_added.strftime('%Y-%m-%d %H:%M:%S')
        
        # Use pipeline for atomic operations
        pipeline = redis_client.client.pipeline(transaction=True)
        
        # Update monthly item totals
        pipeline.eval(
            self._atomic_hash_update_script(),
            1,
            keys['total_items'],
            str(drop.item_id),
            str(drop.quantity),
            str(total_value),
            "false",  # Not force update
            "1",  # Drop count delta
            drop_timestamp,
            drop_timestamp
        )
        
        # Update all-time item totals
        pipeline.eval(
            self._atomic_hash_update_script(),
            1,
            keys['all_time_total_items'],
            str(drop.item_id),
            str(drop.quantity),
            str(total_value),
            "false",
            "1",
            drop_timestamp,
            drop_timestamp
        )
        
        # Update daily item totals
        pipeline.eval(
            self._atomic_hash_update_script(),
            1,
            keys['daily_total_items'],
            str(drop.item_id),
            str(drop.quantity),
            str(total_value),
            "false",
            "1",
            drop_timestamp,
            drop_timestamp
        )
        
        # Update total loot for all granularities
        pipeline.incrbyfloat(keys['total_loot'], total_value)  # Monthly
        pipeline.incrbyfloat(keys['all_time_total_loot'], total_value)  # All-time
        pipeline.incrbyfloat(keys['daily_total_loot'], total_value)  # Daily
        
        if int(drop.value * drop.quantity) > 1000000:
            # Add to recent items
            recent_item_data = {
                'drop_id': drop.drop_id,
                'item_id': drop.item_id,
                'npc_id': drop.npc_id,
                'value': drop.value,
                'quantity': drop.quantity,
                'total_value': total_value,
                'date_added': drop_timestamp,
                'partition': partition
            }
            
            # Add to all granularities
            pipeline.lpush(keys['recent_items'], json.dumps(recent_item_data))  # Monthly
            pipeline.lpush(keys['all_time_recent_items'], json.dumps(recent_item_data))  # All-time
            pipeline.lpush(keys['daily_recent_items'], json.dumps(recent_item_data))  # Daily
            
            # Trim recent items lists
            pipeline.ltrim(keys['recent_items'], 0, 49)  # Keep last 50 items (monthly)
            pipeline.ltrim(keys['all_time_recent_items'], 0, 99)  # Keep last 100 items (all-time)
            pipeline.ltrim(keys['daily_recent_items'], 0, 24)  # Keep last 25 items (daily)
        
        # Execute all operations atomically
        try:
            pipeline.execute()
            return True
        except Exception as e:
            print(f"Pipeline execution failed: {e}")
            return False
    
    def force_update_player(self, player_id: int, session_to_use=None) -> bool:
        """
        Force update a player's Redis cache by recalculating from database.
        This removes all existing Redis data and rebuilds from scratch.
        Thread-safe with concurrency protection.
        """
        with self._lock:
            if player_id in self._processing_players:
                return False  # Already being processed
            
            self._processing_players.add(player_id)
        
        try:
            return self._force_update_player_internal(player_id, session_to_use)
        finally:
            with self._lock:
                self._processing_players.discard(player_id)
    
    def _force_update_player_internal(self, player_id: int, session_to_use=None) -> bool:
        """Internal force update implementation"""
        if session_to_use is None:
            session_to_use = session
        
        try:
            # Get player with groups
            player = session_to_use.query(Player).filter(Player.player_id == player_id).options(joinedload(Player.groups)).first()
            if not player:
                print(f"Player {player_id} not found")
                return False
            
            # Get player's group IDs
            player_group_ids = [group.group_id for group in player.groups]
            print(f"Player {player_id} belongs to groups: {player_group_ids}")
            
            # Get all drops for the player
            player_drops = session_to_use.query(Drop).filter(
                Drop.player_id == player_id
            ).order_by(Drop.date_added.asc()).all()
            
            if not player_drops:
                # No drops, clear Redis data and remove from leaderboards
                self._clear_player_redis_data(player_id)
                self._remove_from_leaderboards(player_id, player_group_ids)
                return True
            
            # Group drops by partition (monthly) and by day
            partition_drops = {}  # monthly partitions
            daily_drops = {}      # daily partitions
            
            for drop in player_drops:
                # Monthly partition
                partition = drop.partition
                if partition not in partition_drops:
                    partition_drops[partition] = []
                partition_drops[partition].append(drop)
                
                # Daily partition
                daily_partition = drop.date_added.strftime('%Y%m%d')
                if daily_partition not in daily_drops:
                    daily_drops[daily_partition] = []
                daily_drops[daily_partition].append(drop)
            
            # Clear existing Redis data
            self._clear_player_redis_data(player_id)
            self._remove_from_leaderboards(player_id, player_group_ids)
            
            # Rebuild Redis data for each monthly partition and update leaderboards
            for partition, drops in partition_drops.items():
                total_loot = self._rebuild_partition_data(player_id, partition, drops)
                # Update leaderboards for this partition
                self.update_leaderboards(player_id, total_loot, partition, player_group_ids)
                print(f"Updated leaderboards for player {player_id} in partition {partition}")
            
            # Rebuild Redis data for each daily partition
            for daily_partition, drops in daily_drops.items():
                self._rebuild_daily_data(player_id, daily_partition, drops)
                print(f"Updated daily data for player {player_id} on {daily_partition}")
            # Update player's last update timestamp
            player.date_updated = datetime.now()
            session_to_use.commit()
            
            return True
            
        except Exception as e:
            print(f"Force update failed for player {player_id}: {e}")
            return False
    
    def _clear_player_redis_data(self, player_id: int):
        """Clear all Redis data for a player"""
        # Get all keys for this player
        pattern = f"player:{player_id}:*"
        keys = redis_client.client.keys(pattern)
        
        if keys:
            redis_client.client.delete(*keys)
    
    def _remove_from_leaderboards(self, player_id: int, group_ids: List[int]):
        """Remove player from all leaderboards"""
        current_partition = self._get_partition()
        
        pipeline = redis_client.client.pipeline(transaction=True)
        
        # Remove from global leaderboard
        global_key = f"leaderboard:{current_partition}"
        pipeline.zrem(global_key, player_id)
        
        # Remove from group leaderboards
        for group_id in group_ids:
            group_key = f"leaderboard:{current_partition}:group:{group_id}"
            pipeline.zrem(group_key, player_id)
        
        pipeline.execute()
    
    def _rebuild_partition_data(self, player_id: int, partition: int, drops: List[Drop]) -> int:
        """Rebuild Redis data for a specific partition. Returns total loot value."""
        keys = self._get_redis_keys(player_id, partition)
        
        # Aggregate data
        item_data = {}  # item_id -> (quantity, total_value, drop_count, first_drop, last_drop)
        total_loot = 0
        recent_items_raw = []
        
        for drop in drops:
            total_value = drop.value * drop.quantity
            total_loot += total_value
            drop_timestamp = drop.date_added.strftime('%Y-%m-%d %H:%M:%S')
            
            # Aggregate item data
            if drop.item_id not in item_data:
                item_data[drop.item_id] = (0, 0, 0, drop_timestamp, drop_timestamp)
            
            qty, val, count, first, _ = item_data[drop.item_id]
            item_data[drop.item_id] = (qty + drop.quantity, val + total_value, count + 1, first, drop_timestamp)
            if int(drop.value * drop.quantity) > 1000000:
                # Add to recent items
                recent_item_data = {
                    'drop_id': drop.drop_id,
                    'item_id': drop.item_id,
                    'npc_id': drop.npc_id,
                    'value': drop.value,
                    'quantity': drop.quantity,
                    'total_value': total_value,
                    'date_added': drop_timestamp,
                    'partition': partition }
                recent_items_raw.append(recent_item_data)
        
        # Use pipeline for atomic updates
        pipeline = redis_client.client.pipeline(transaction=True)
        
        # Set total loot
        pipeline.set(keys['total_loot'], total_loot)
        pipeline.set(keys['all_time_total_loot'], total_loot)

        recent_items_raw.sort(key=lambda x: x['date_added'])
        recent_items = [json.dumps(item) for item in recent_items_raw]
        
        # Set item data
        for item_id, (qty, val, count, first, last) in item_data.items():
            item_value = f"{qty},{val},{count},{first},{last}"
            pipeline.hset(keys['total_items'], item_id, item_value)
            pipeline.hset(keys['all_time_total_items'], item_id, item_value)
        
        # Set recent items
        if recent_items_raw:
            pipeline.delete(keys['recent_items'])
            pipeline.delete(keys['all_time_recent_items'])
            pipeline.lpush(keys['recent_items'], *recent_items)  # Use recent_items, not recent_items_raw
            pipeline.lpush(keys['all_time_recent_items'], *recent_items)  # Use recent_items, not recent_items_raw
        
        # Execute all operations
        pipeline.execute()
        
        return total_loot
    
    def _rebuild_daily_data(self, player_id: int, daily_partition: str, drops: List[Drop]) -> int:
        """Rebuild Redis data for a specific daily partition. Returns total loot value."""
        # Generate daily keys
        daily_keys = {
            'daily_total_items': f"player:{player_id}:daily:{daily_partition}:total_items",
            'daily_total_loot': f"player:{player_id}:daily:{daily_partition}:total_loot",
            'daily_recent_items': f"player:{player_id}:daily:{daily_partition}:recent_items",
            'daily_drop_history': f"player:{player_id}:daily:{daily_partition}:drop_history",
            'daily_high_value_items': f"player:{player_id}:daily:{daily_partition}:high_value_items"
        }
        
        # Aggregate data for this day
        item_data = {}  # item_id -> (quantity, total_value, drop_count, first_drop, last_drop)
        total_loot = 0
        recent_items_raw = []
        
        for drop in drops:
            total_value = drop.value * drop.quantity
            total_loot += total_value
            drop_timestamp = drop.date_added.strftime('%Y-%m-%d %H:%M:%S')
            
            # Aggregate item data
            if drop.item_id not in item_data:
                item_data[drop.item_id] = (0, 0, 0, drop_timestamp, drop_timestamp)
            
            qty, val, count, first, _ = item_data[drop.item_id]
            item_data[drop.item_id] = (qty + drop.quantity, val + total_value, count + 1, first, drop_timestamp)
            
            if int(drop.value * drop.quantity) > 1000000:
                # Add to recent items
                recent_item_data = {
                    'drop_id': drop.drop_id,
                    'item_id': drop.item_id,
                    'npc_id': drop.npc_id,
                    'value': drop.value,
                    'quantity': drop.quantity,
                    'total_value': total_value,
                    'date_added': drop_timestamp,
                    'daily_partition': daily_partition
                }
                recent_items_raw.append(recent_item_data)
        
        # Use pipeline for atomic updates
        pipeline = redis_client.client.pipeline(transaction=True)
        
        # Set daily total loot
        pipeline.set(daily_keys['daily_total_loot'], total_loot)
        
        # Sort recent items by time
        recent_items_raw.sort(key=lambda x: x['date_added'])
        recent_items = [json.dumps(item) for item in recent_items_raw]
        
        # Set daily item data
        for item_id, (qty, val, count, first, last) in item_data.items():
            item_value = f"{qty},{val},{count},{first},{last}"
            pipeline.hset(daily_keys['daily_total_items'], item_id, item_value)
        
        # Set daily recent items
        if recent_items_raw:
            pipeline.delete(daily_keys['daily_recent_items'])
            pipeline.lpush(daily_keys['daily_recent_items'], *recent_items)
        
        # Set expiration for daily keys (optional - expire after 90 days to save memory)
        expiration_days = 90
        expiration_seconds = expiration_days * 24 * 60 * 60
        for key in daily_keys.values():
            pipeline.expire(key, expiration_seconds)
        
        # Execute all operations
        pipeline.execute()
        
        return total_loot
    
    def generate_loot_leaderboard(self, query: LootLeaderboardQuery) -> Dict:
        """
        Generate a comprehensive loot leaderboard based on query parameters.
        Returns aggregated data for all matching players.
        """
        result = {
            'players': [],
            'total_players': 0,
            'total_value': 0,
            'high_value_items': [],
            'generated_at': datetime.now().isoformat()
        }
        
        # Get player IDs to process
        if query.player_ids:
            player_ids = query.player_ids
        else:
            # Get all players from database
            players = session.query(Player.player_id).all()
            player_ids = [p[0] for p in players]
        
        # Process each player
        for player_id in player_ids:
            player_summary = self._get_player_loot_summary(
                player_id, query.npc_ids, query.start_time, 
                query.end_time, query.min_item_value, query.partition
            )
            
            if player_summary:
                result['players'].append({
                    'player_id': player_summary.player_id,
                    'total_value': player_summary.total_value,
                    'total_drops': player_summary.total_drops,
                    'unique_items': player_summary.unique_items,
                    'unique_npcs': player_summary.unique_npcs,
                    'items': len(player_summary.items),
                    'high_value_items': len(player_summary.high_value_items)
                })
                
                result['total_value'] += player_summary.total_value
                result['high_value_items'].extend(player_summary.high_value_items)
        
        # Sort players by total value
        result['players'].sort(key=lambda x: x['total_value'], reverse=True)
        result['total_players'] = len(result['players'])
        
        # Sort high value items by value
        result['high_value_items'].sort(key=lambda x: x['total_value'], reverse=True)
        
        return result
    
    def _get_player_loot_summary(self, player_id: int, npc_ids: Optional[List[int]] = None,
                                start_time: Optional[datetime] = None, 
                                end_time: Optional[datetime] = None,
                                min_item_value: Optional[int] = None,
                                partition: Optional[int] = None) -> Optional[PlayerLootSummary]:
        """Get comprehensive loot summary for a player"""
        
        if partition is None:
            partition = self._get_partition()
        
        keys = self._get_redis_keys(player_id, partition)
        
        # Get total loot
        total_loot_str = redis_client.get(keys['total_loot'])
        if not total_loot_str:
            return None
        
        total_loot = int(float(total_loot_str))
        
        # Get item data
        items_data = redis_client.client.hgetall(keys['total_items'])
        items = {}
        total_drops = 0
        unique_npcs = set()
        high_value_items = []
        
        for item_id_bytes, item_data_bytes in items_data.items():
            item_id = int(item_id_bytes.decode('utf-8'))
            item_data = item_data_bytes.decode('utf-8').split(',')
            
            if len(item_data) >= 5:
                quantity = int(item_data[0])
                total_value = int(item_data[1])
                drop_count = int(item_data[2])
                first_drop = datetime.strptime(item_data[3], '%Y-%m-%d %H:%M:%S')
                last_drop = datetime.strptime(item_data[4], '%Y-%m-%d %H:%M:%S')
                
                # Apply filters
                if start_time and last_drop < start_time:
                    continue
                if end_time and first_drop > end_time:
                    continue
                
                # Check for high value items
                if min_item_value and total_value >= min_item_value:
                    high_value_items.append({
                        'item_id': item_id,
                        'quantity': quantity,
                        'total_value': total_value,
                        'drop_count': drop_count,
                        'first_drop': first_drop.isoformat(),
                        'last_drop': last_drop.isoformat()
                    })
                
                items[item_id] = PlayerItemData(
                    item_id=item_id,
                    quantity=quantity,
                    total_value=total_value,
                    drop_count=drop_count,
                    first_drop=first_drop,
                    last_drop=last_drop
                )
                
                total_drops += drop_count
        
        return PlayerLootSummary(
            player_id=player_id,
            total_value=total_loot,
            total_drops=total_drops,
            unique_items=len(items),
            unique_npcs=len(unique_npcs),
            items=items,
            high_value_items=high_value_items
        )
    
    def get_player_rank(self, player_id: int, group_id: Optional[int] = None, 
                       partition: Optional[int] = None) -> Optional[Tuple[int, int]]:
        """
        Get a player's rank and total players in ranking.
        Returns (rank, total_players) or None if not found.
        """
        if partition is None:
            partition = self._get_partition()
        
        # Get leaderboard key
        if group_id:
            rank_key = f"leaderboard:{partition}:group:{group_id}"
        else:
            rank_key = f"leaderboard:{partition}"
        
        # Get player's score and rank
        score = redis_client.client.zscore(rank_key, player_id)
        if score is None:
            return None
        
        rank = redis_client.client.zrevrank(rank_key, player_id)
        total_players = redis_client.client.zcard(rank_key)
        
        if rank is None:
            return None
        
        return (int(rank) + 1, total_players)  # Redis ranks are 0-based
    
    def update_leaderboards(self, player_id: int, total_value: int, 
                           partition: Optional[int] = None, group_ids: Optional[List[int]] = None):
        """Update leaderboards for a player"""
        if partition is None:
            partition = self._get_partition()
        
        pipeline = redis_client.client.pipeline(transaction=True)
        
        # Update global leaderboard
        global_key = f"leaderboard:{partition}"
        pipeline.zadd(global_key, {player_id: total_value})
        
        # Update group leaderboards
        if group_ids:
            for group_id in group_ids:
                group_key = f"leaderboard:{partition}:group:{group_id}"
                pipeline.zadd(group_key, {player_id: total_value})
                print(f"Updated group leaderboard {group_id} for player {player_id} with value {total_value:,}")
        
        pipeline.execute()

# Global instance
loot_tracker = RedisLootTracker()


def get_player_current_month_total(player_id: int) -> int:
    """Fetch the player's monthly total loot from Redis computed by redis_updates."""
    try:
        now = datetime.now()
        partition = now.year * 100 + now.month
        key = f"player:{player_id}:{partition}:total_loot"
        total_str = redis_client.get(key)
        if total_str is None:
            # Fallback to global leaderboard score if key missing
            score = redis_client.client.zscore(f"leaderboard:{partition}", player_id)
            return int(float(score)) if score is not None else 0
        return int(float(total_str))
    except Exception:
        return 0
    
def get_player_list_loot_sum(player_ids: List[int]):
    try:
        group_total = 0
        for player_id in player_ids:
            group_total += get_player_current_month_total(player_id)
        return group_total
    except Exception:
        return 0

# Convenience functions for backward compatibility
def add_to_player(player: Player, drop: Drop) -> bool:
    """Add a drop to a player's Redis cache"""
    return loot_tracker.add_to_player(player, drop)

def force_update_player(player_id: int, session_to_use=None) -> bool:
    """Force update a player's Redis cache from database"""
    return loot_tracker.force_update_player(player_id, session_to_use)

def generate_loot_leaderboard(query: LootLeaderboardQuery) -> Dict:
    """Generate loot leaderboard based on query parameters"""
    return loot_tracker.generate_loot_leaderboard(query)


if __name__ == "__main__":
    force_update_player(795, session)