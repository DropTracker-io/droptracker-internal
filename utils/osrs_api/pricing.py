"""
Pricing API for RuneScape Wiki Prices API.

This module handles all interactions with the RuneScape Wiki's real-time
Grand Exchange pricing API.
"""

from typing import Optional, Dict, Any


class PricingAPI:
    """
    API client for RuneScape Wiki pricing data.
    """
    
    PRICES_API_BASE = "https://prices.runescape.wiki/api/v1/osrs"
    
    def __init__(self, client):
        """Initialize with reference to main client."""
        self.client = client
    
    async def get_mapping(self) -> Optional[Dict[str, Any]]:
        """
        Fetch the item mapping data which contains names, IDs, and other metadata.
        
        Returns:
            Dictionary containing item mapping data, or None if failed
        """
        try:
            endpoint = f"{self.PRICES_API_BASE}/mapping"
            session = await self.client.get_prices_session()
            
            async with session.get(endpoint) as resp:
                if resp.status != 200:
                    return None
                return await resp.json()
        except Exception as e:
            print(f"Error fetching mapping data: {e}")
            return None
    
    async def find_item_id_by_name(self, name: str) -> Optional[int]:
        """
        Find an item ID by name using the mapping data.
        
        Args:
            name: Item name to search for
            
        Returns:
            Item ID as integer, or None if not found
        """
        try:
            mapping_data = await self.get_mapping()
            if not mapping_data:
                return None
            
            name_lower = name.lower()
            for item in mapping_data:
                if item.get('name', '').lower() == name_lower:
                    return item['id']
            return None
        except Exception as e:
            print(f"Error finding item ID for {name}: {e}")
            return None
    
    async def get_latest_price_data(self, item_id: int) -> Optional[Dict[str, Any]]:
        """
        Fetch the latest price data from the real-time prices API.
        
        Args:
            item_id: The item ID to get price data for
            
        Returns:
            Dictionary containing price data, or None if failed
        """
        try:
            endpoint = f"{self.PRICES_API_BASE}/latest"
            params = {'id': item_id}
            session = await self.client.get_prices_session()
            
            async with session.get(endpoint, params=params) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                
                if 'data' not in data:
                    return None
                
                item_data = data['data'].get(str(item_id))
                if not item_data:
                    return None
                
                return item_data
        except Exception as e:
            print(f"Error fetching price data for item {item_id}: {e}")
            return None
    
    async def get_most_recent_price_by_id(self, item_id: int) -> Optional[int]:
        """
        Get the most recent price for an item by ID.
        
        Args:
            item_id: The item ID to get price for
            
        Returns:
            The price as an integer, or None if not found
        """
        try:
            if not item_id:
                return None
            
            price_data = await self.get_latest_price_data(item_id)
            if not price_data:
                return None
            
            high_price = price_data.get('high')
            low_price = price_data.get('low')
            high_time = price_data.get('highTime')
            low_time = price_data.get('lowTime')
            
            # Determine the most recent price
            if high_price and low_price and high_time and low_time:
                if high_time > low_time:
                    return high_price
                else:
                    return low_price
            elif high_price and high_time:
                return high_price
            elif low_price and low_time:
                return low_price
            
            return None
        except Exception as e:
            print(f"Error getting recent price for item {item_id}: {e}")
            return None
    
    async def get_most_recent_price_by_name(self, item_name: str) -> Optional[int]:
        """
        Get the most recent price for an item by name.
        
        Args:
            item_name: The item name to get price for
            
        Returns:
            The price as an integer, or None if not found
        """
        try:
            item_id = await self.find_item_id_by_name(item_name)
            if not item_id:
                return None
            
            return await self.get_most_recent_price_by_id(item_id)
        except Exception as e:
            print(f"Error getting recent price for {item_name}: {e}")
            return None
    
    async def get_true_item_value(self, item_name: str, provided_value: int = 0) -> int:
        """
        Get the true value of an item, accounting for untradeable items that have indirect value.
        
        For example, a vestige has untradeable drop value, but is actually worth
        the corresponding ring minus 3 Chromium Ingots.
        
        Args:
            item_name: Name of the item
            provided_value: Fallback value if no calculation can be made
            
        Returns:
            The calculated true value or the provided fallback value
        """
        try:
            item_lower = item_name.lower()
            
            # Vestige calculations
            if "vestige" in item_lower:
                ring = item_lower.replace("vestige", "ring")
                ring_price = await self.get_most_recent_price_by_name(ring)
                ingot_price = await self.get_most_recent_price_by_name("Chromium ingot")
                return ring_price - (ingot_price * 3) if ring_price and ingot_price else provided_value
            
            # Bludgeon piece calculations
            if "bludgeon" in item_lower:
                if item_lower in ["bludgeon axon", "bludgeon claw", "bludgeon spine"]:
                    bludgeon_value = await self.get_most_recent_price_by_name("Abyssal bludgeon")
                    return int(bludgeon_value / 3) if bludgeon_value else provided_value
                else:
                    return provided_value
            
            # Hydra piece calculations
            if item_lower in ["hydra's eye", "hydra's fang", "hydra's heart"]:
                brimstone_value = await self.get_most_recent_price_by_name("Brimstone ring")
                return int(brimstone_value / 3) if brimstone_value else provided_value
            
            # Noxious halberd piece calculations
            if "noxious" in item_lower:
                noxious_halberd_value = await self.get_most_recent_price_by_name("Noxious halberd")
                if any(part in item_lower for part in ["point", "blade", "pommel"]):
                    return int(noxious_halberd_value / 3) if noxious_halberd_value else provided_value
                else:
                    return provided_value
            
            # Araxyte fang calculation
            if item_lower == "araxyte fang":
                amulet_of_rancour_value = await self.get_most_recent_price_by_name("Amulet of rancour")
                torture_value = await self.get_most_recent_price_by_name("Amulet of torture")
                if amulet_of_rancour_value and torture_value:
                    return amulet_of_rancour_value - torture_value
                else:
                    return provided_value
            
            # Mokhaiotl cloth calculation
            if item_lower == "mokhaiotl cloth":
                tormented_bracelet_value = await self.get_most_recent_price_by_name("Tormented bracelet")
                demon_tear_value = await self.get_most_recent_price_by_name("Demon tear")
                confliction_gauntlet_value = await self.get_most_recent_price_by_name("Confliction gauntlets")
                if confliction_gauntlet_value and tormented_bracelet_value and demon_tear_value:
                    return confliction_gauntlet_value - tormented_bracelet_value - (demon_tear_value * 10000)
                else:
                    return 5000000
            
            # Default case - return provided value
            return provided_value
            
        except Exception as e:
            print(f"Error calculating true value for {item_name}: {e}")
            return provided_value
