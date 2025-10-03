"""
Semantic API for OSRS Wiki data using the new Bucket API.

This module handles all interactions with the OSRS Wiki's Bucket API,
replacing the deprecated Semantic MediaWiki (SMW) API.
"""

import json
import html
from typing import Dict, List, Optional, Union, Any
from urllib.parse import quote


class SemanticAPI:
    """
    API client for OSRS Wiki semantic data using the Bucket API.
    """
    
    WIKI_API_URL = 'https://oldschool.runescape.wiki/api.php'
    
    # Mapping of database names to semantic names for compatibility
    ALT_NAMES = {
        # Semantic name -> our database name
        "Rewards Chest (Fortis Colosseum)": "Fortis Colosseum",
        "Ancient chest": ["Chambers of Xeric", "Chambers of Xeric Challenge Mode"],
        "Monumental chest": ["Theatre of Blood: Hard Mode", "Theatre of Blood"],
        "Chest (Tombs of Amascut)": ["Tombs of Amascut", "Tombs of Amascut: Expert Mode"],
        "Chest (Barrows)": "Barrows",
        "Reward pool": "Tempoross",
        "Reward casket (easy)": "Clue Scroll (Easy)",
        "Reward casket (medium)": "Clue Scroll (Medium)",
        "Reward casket (hard)": "Clue Scroll (Hard)",
        "Reward casket (elite)": "Clue Scroll (Elite)",
        "Reward casket (master)": "Clue Scroll (Master)",
        "Reward Chest (The Gauntlet)": "Corrupted Gauntlet"
    }
    
    def __init__(self, client):
        """Initialize with reference to main client."""
        self.client = client
        self._ca_tiers_cache = None  # Cache for Combat Achievement tiers
    
    async def _bucket_query(self, query: str) -> Dict[str, Any]:
        """
        Execute a bucket query against the OSRS Wiki API.
        
        Args:
            query: The bucket query string
            
        Returns:
            Dictionary containing the API response
        """
        session = await self.client.get_wiki_session()
        
        params = {
            'format': 'json',
            'action': 'bucket',
            'query': query,
            'formatversion': '2'
        }
        
        async with session.get(self.WIKI_API_URL, params=params) as resp:
            if resp.status != 200:
                return {}
            
            body = await resp.json()
            if 'error' in body:
                print(f"Bucket API error: {body['error']}")
                return {}
            
            return body
    
    async def get_item_id(self, item_name: str) -> Optional[int]:
        """
        Look up an item ID from the OSRS Wiki using the Bucket API.
        
        Args:
            item_name: The name of the item to look up
            
        Returns:
            The first item ID as an integer, or None if not found
        """
        try:
            # Escape the item name for the query
            escaped_name = quote(item_name)
            query = f"bucket('infobox_item').select('item_id').where('item_name', '{item_name}').run()"
            
            result = await self._bucket_query(query)
            bucket_data = result.get('bucket', [])
            
            if bucket_data:
                # Get the first item's ID
                first_item = bucket_data[0]
                item_ids = first_item.get('item_id', [])
                if item_ids:
                    return int(item_ids[0])
            
            return None
        except Exception as e:
            print(f"Error getting item ID for {item_name}: {e}")
            return None
    
    async def get_npc_id(self, npc_name: str) -> Optional[int]:
        """
        Look up an NPC ID from the OSRS Wiki using the Bucket API.
        
        Args:
            npc_name: The name of the NPC to look up
            
        Returns:
            The first matching NPC ID as an integer, or None if not found
        """
        try:
            # Handle special cases
            if npc_name == "Corrupted Gauntlet":
                return 9035
            
            query = f"bucket('infobox_monster').select('id').where('name', '{npc_name}').run()"
            
            result = await self._bucket_query(query)
            bucket_data = result.get('bucket', [])
            
            if bucket_data:
                # Get the first NPC's ID
                first_npc = bucket_data[0]
                npc_ids = first_npc.get('id', [])
                if npc_ids:
                    return int(npc_ids[0])
            
            return None
        except Exception as e:
            print(f"Error getting NPC ID for {npc_name}: {e}")
            return None
    
    async def check_item_exists(self, item_name: str) -> bool:
        """
        Check if an item exists in the OSRS Wiki database.
        
        Args:
            item_name: The name of the item to check
            
        Returns:
            True if the item exists, False otherwise
        """
        item_id = await self.get_item_id(item_name)
        return item_id is not None
    
    async def check_drop(self, item_name: str, npc_name: str) -> bool:
        """
        Check if an item drops from a specific NPC.
        
        Args:
            item_name: The name of the item
            npc_name: The name of the NPC
            
        Returns:
            True if the item drops from the NPC, False otherwise
        """
        try:
            # Handle special cases
            if item_name == "Enhanced crystal teleport seed" and npc_name == "Elf":
                return True
            if item_name.strip() == "Black tourmaline core" and npc_name.strip() == "Dusk":
                return True
            
            # Create reverse mapping for alternative names
            reverse_alt_names = {}
            for semantic_name, db_names in self.ALT_NAMES.items():
                if isinstance(db_names, list):
                    for db_name in db_names:
                        reverse_alt_names[db_name] = semantic_name
                else:
                    reverse_alt_names[db_names] = semantic_name
            
            # Get the semantic name if it exists in our mapping
            semantic_name = reverse_alt_names.get(npc_name, npc_name)
            if semantic_name != npc_name:
                print(f"Using semantic name: {semantic_name} for {npc_name}")
            
            # Query the dropsline bucket to find NPCs that drop this item
            query = f"bucket('dropsline').select('page_name').where('item_name', '{item_name}').run()"
            
            result = await self._bucket_query(query)
            bucket_data = result.get('bucket', [])
            
            # Check if any of the returned NPCs match our target NPC
            for drop_entry in bucket_data:
                dropped_from = drop_entry.get('page_name', '')
                
                # Remove any subpage references (e.g., "NPC name#Normal")
                if "#" in dropped_from:
                    dropped_from = dropped_from.split("#")[0]
                
                # Check if this drop source matches our NPC name
                if dropped_from.lower() == semantic_name.lower():
                    print(f"Drop found & valid for {item_name} from {dropped_from}")
                    return True
            
            print(f"No valid drop found for {item_name} from {semantic_name}")
            return False
            
        except Exception as e:
            print(f"Error checking drop for {item_name} from {npc_name}: {e}")
            return False
    
    async def find_related_drops(self, item_name: str, npc_name: str) -> Dict[str, Any]:
        """
        Find all items that drop from a specific NPC.
        
        Args:
            item_name: Target item name (for context)
            npc_name: The NPC to find drops for
            
        Returns:
            Dictionary containing all drops from the NPC
        """
        try:
            # Create reverse mapping for alternative names
            reverse_alt_names = {}
            for semantic_name, db_names in self.ALT_NAMES.items():
                if isinstance(db_names, list):
                    for db_name in db_names:
                        reverse_alt_names[db_name] = semantic_name
                else:
                    reverse_alt_names[db_names] = semantic_name
            
            # Get the semantic name if it exists in our mapping
            semantic_name = reverse_alt_names.get(npc_name, npc_name)
            
            # Query all drops from this NPC
            query = f"bucket('dropsline').select('item_name', 'page_name').where('page_name', '{semantic_name}').run()"
            
            result = await self._bucket_query(query)
            bucket_data = result.get('bucket', [])
            
            all_drops = []
            for drop_entry in bucket_data:
                dropped_item = drop_entry.get('item_name', '')
                dropped_from = drop_entry.get('page_name', '')
                
                # Remove any subpage references
                if "#" in dropped_from:
                    dropped_from = dropped_from.split("#")[0]
                
                if dropped_from.lower() == semantic_name.lower():
                    all_drops.append({
                        "item_name": dropped_item,
                        "rarity": "Unknown",  # Rarity not available in dropsline bucket
                        "npc_name": dropped_from
                    })
            
            return {
                "target_item": item_name,
                "npc_name": semantic_name,
                "all_drops": all_drops
            }
            
        except Exception as e:
            print(f"Error finding related drops for {npc_name}: {e}")
            return {
                "target_item": item_name,
                "npc_name": npc_name,
                "all_drops": []
            }
    
    async def get_global_value(self, variable: str) -> Optional[str]:
        """
        Get a global variable value from the OSRS Wiki.
        
        Args:
            variable: The global variable name
            
        Returns:
            The variable value as a string, or None if not found
        """
        try:
            session = await self.client.get_wiki_session()
            
            params = {
                'format': 'json',
                'action': 'expandtemplates',
                'text': f'{{{{Globals|{variable}}}}}',
                'prop': 'wikitext'
            }
            
            async with session.get(self.WIKI_API_URL, params=params) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get('expandtemplates', {}).get('wikitext')
            
            return None
        except Exception as e:
            print(f"Error getting global value for {variable}: {e}")
            return None
    
    async def get_combat_achievement_tiers(self) -> Dict[str, Dict[str, str]]:
        """
        Get combat achievement tier information with caching.
        
        Returns:
            Dictionary containing tier data with tasks and points
        """
        # Return cached data if available
        if self._ca_tiers_cache is not None:
            return self._ca_tiers_cache
        
        # Map short names to full names
        tier_mapping = {
            'easy': 'Easy',
            'medium': 'Medium',
            'hard': 'Hard',
            'elite': 'Elite',
            'master': 'Master',
            'gm': 'Grandmaster'
        }
        
        tier_data = {}
        
        # Use full names when setting the values
        for short_name, full_name in tier_mapping.items():
            tier_data[full_name] = {
                'tasks': await self.get_global_value(f'ca {short_name} tasks'),
                'task_points': await self.get_global_value(f'ca {short_name} task points'),
                'total_points': await self.get_global_value(f'ca {short_name} points')
            }
        
        # Get total tasks
        tier_data['Total'] = {
            'tasks': await self.get_global_value('ca total tasks'),
        }
        
        # Cache the result
        self._ca_tiers_cache = tier_data
        return tier_data
    
    async def get_ca_tier_progress(self, current_points: int) -> tuple[float, int]:
        """
        Calculate combat achievement tier progress.
        
        Args:
            current_points: Current CA points
            
        Returns:
            Tuple of (progress_percentage, next_tier_points)
        """
        current_points = int(current_points)
        tiers = await self.get_combat_achievement_tiers()
        
        # Define tier order from lowest to highest
        tier_order = ['Easy', 'Medium', 'Hard', 'Elite', 'Master', 'Grandmaster']
        
        # Find which tier the player is currently in and what's next
        current_tier = None
        next_tier = None
        current_tier_points = 0
        next_tier_points = 0
        
        # First find the current tier
        for i, tier_name in enumerate(tier_order):
            if tier_name not in tiers or not tiers[tier_name]['total_points']:
                continue
            
            tier_points = int(tiers[tier_name]['total_points'])
            
            if current_points >= tier_points:
                current_tier = tier_name
                current_tier_points = tier_points
                # Look ahead to next tier
                if i + 1 < len(tier_order) and tier_order[i + 1] in tiers:
                    next_tier = tier_order[i + 1]
                    if tiers[next_tier]['total_points']:
                        next_tier_points = int(tiers[next_tier]['total_points'])
            else:
                # If we haven't reached this tier, it's our next goal
                if current_tier is None:
                    next_tier = tier_name
                    next_tier_points = tier_points
                    current_tier_points = 0
                break
        
        # Calculate progress
        if current_tier is None:
            # Haven't reached Easy tier yet
            if 'Easy' in tiers and tiers['Easy']['total_points']:
                easy_points = int(tiers['Easy']['total_points'])
                progress = (current_points / easy_points) * 100
                return round(progress, 2), easy_points
            return 0.0, 0
        elif next_tier is None:
            # Completed Grandmaster
            if 'Grandmaster' in tiers and tiers['Grandmaster']['total_points']:
                return 100.0, int(tiers['Grandmaster']['total_points'])
            return 100.0, current_tier_points
        else:
            # Calculate progress to next tier
            points_needed = next_tier_points - current_tier_points
            if points_needed == 0:
                return 100.0, next_tier_points
            points_gained = current_points - current_tier_points
            try:
                progress = (points_gained / points_needed) * 100
                return round(progress, 2), next_tier_points
            except Exception as e:
                print(f"Error calculating CA progress: {e}")
                return 0.0, next_tier_points
    
    async def get_current_ca_tier(self, current_points: int) -> Optional[str]:
        """
        Get the current combat achievement tier for given points.
        
        Args:
            current_points: Current CA points
            
        Returns:
            Current tier name or None
        """
        current_points = int(current_points)
        tiers = await self.get_combat_achievement_tiers()
        
        # Define tier order from highest to lowest
        tier_order = ['Grandmaster', 'Master', 'Elite', 'Hard', 'Medium', 'Easy']
        
        # Check tiers in descending order
        for tier_name in tier_order:
            if tier_name not in tiers or not tiers[tier_name]['total_points']:
                continue
            
            tier_points = int(tiers[tier_name]['total_points'])
            if current_points >= tier_points:
                return tier_name
        
        return None
    
    async def get_ca_info(self, current_points: int) -> Dict[str, Any]:
        """
        Get complete Combat Achievement information for given points in a single call.
        
        This is more efficient than calling get_current_ca_tier() and get_ca_tier_progress()
        separately as it only fetches the tier data once.
        
        Args:
            current_points: Current CA points
            
        Returns:
            Dictionary containing current tier, progress, and next tier info
        """
        current_points = int(current_points)
        tiers = await self.get_combat_achievement_tiers()
        
        # Define tier order from lowest to highest
        tier_order = ['Easy', 'Medium', 'Hard', 'Elite', 'Master', 'Grandmaster']
        
        # Find current and next tier
        current_tier = None
        next_tier = None
        current_tier_points = 0
        next_tier_points = 0
        
        # Find the current tier
        for i, tier_name in enumerate(tier_order):
            if tier_name not in tiers or not tiers[tier_name]['total_points']:
                continue
            
            tier_points = int(tiers[tier_name]['total_points'])
            
            if current_points >= tier_points:
                current_tier = tier_name
                current_tier_points = tier_points
                # Look ahead to next tier
                if i + 1 < len(tier_order) and tier_order[i + 1] in tiers:
                    next_tier = tier_order[i + 1]
                    if tiers[next_tier]['total_points']:
                        next_tier_points = int(tiers[next_tier]['total_points'])
            else:
                # If we haven't reached this tier, it's our next goal
                if current_tier is None:
                    next_tier = tier_name
                    next_tier_points = tier_points
                    current_tier_points = 0
                break
        
        # Calculate progress
        if current_tier is None:
            # Haven't reached Easy tier yet
            if 'Easy' in tiers and tiers['Easy']['total_points']:
                easy_points = int(tiers['Easy']['total_points'])
                progress = (current_points / easy_points) * 100
                return {
                    'current_tier': None,
                    'next_tier': 'Easy',
                    'progress_percentage': round(progress, 2),
                    'current_points': current_points,
                    'current_tier_points': 0,
                    'next_tier_points': easy_points
                }
        elif next_tier is None:
            # Completed Grandmaster
            return {
                'current_tier': 'Grandmaster',
                'next_tier': None,
                'progress_percentage': 100.0,
                'current_points': current_points,
                'current_tier_points': current_tier_points,
                'next_tier_points': current_tier_points
            }
        else:
            # Calculate progress to next tier
            points_needed = next_tier_points - current_tier_points
            if points_needed == 0:
                progress = 100.0
            else:
                points_gained = current_points - current_tier_points
                progress = (points_gained / points_needed) * 100
            
            return {
                'current_tier': current_tier,
                'next_tier': next_tier,
                'progress_percentage': round(progress, 2),
                'current_points': current_points,
                'current_tier_points': current_tier_points,
                'next_tier_points': next_tier_points
            }
        
        # Fallback
        return {
            'current_tier': None,
            'next_tier': None,
            'progress_percentage': 0.0,
            'current_points': current_points,
            'current_tier_points': 0,
            'next_tier_points': 0
        }
