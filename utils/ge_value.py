import aiohttp
import asyncio
from datetime import datetime
import sys

# Base URLs for the APIs
PRICES_API_BASE = "https://prices.runescape.wiki/api/v1/osrs"
WIKI_API_BASE = "https://oldschool.runescape.wiki/api.php"

# Create a single aiohttp session for reuse
prices_session = None
wiki_session = None

async def get_prices_session():
    global prices_session
    if prices_session is None or prices_session.closed:
        prices_session = aiohttp.ClientSession(headers={
            'User-Agent': 'DropTracker.io - GE Price API Integration - @joelhalen'
        })
    return prices_session

async def get_wiki_session():
    global wiki_session
    if wiki_session is None or wiki_session.closed:
        wiki_session = aiohttp.ClientSession(headers={
            'User-Agent': 'DropTracker.io - GE Price API Integration - @joelhalen'
        })
    return wiki_session

async def get_true_item_value(item_name, provided_value: int = 0):
    # Check if an incoming item matches our defined list of
    # untradeables or otherwise unvalued items that hold a value indirectly
    # for example, an ultor vestige has a 5M untradeable drop value, but actually is worth
    # An ultor ring, minus 3 Chromium Ingots
    item_lower = item_name.lower()
    if "vestige" in item_lower:
        ring = item_lower.replace("vestige", "ring")
        ring_price = await get_most_recent_price_by_name(ring)
        ingot_price = await get_most_recent_price_by_name("Chromium ingot")
        return ring_price - (ingot_price * 3) if ring_price and ingot_price else provided_value
    if "bludgeon" in item_lower:
        if item_lower == "bludgeon axon" or item_lower == "bludgeon claw" or item_lower == "bludgeon spine":
            bludgeon_value = await get_most_recent_price_by_name("Abyssal bludgeon")
            return int(bludgeon_value / 3) if bludgeon_value else provided_value
        else:
            return provided_value
    if item_lower == "hydra's eye" or item_lower == "hydra's fang" or item_lower == "hydra's heart":
        brimstone_value = await get_most_recent_price_by_name("Brimstone ring")
        return int(brimstone_value / 3) if brimstone_value else provided_value
    if "noxious" in item_lower:
        noxious_halberd_value = await get_most_recent_price_by_name("Noxious halberd")
        if "point" in item_lower or "blade" in item_lower or "pommel" in item_lower:
            return int(noxious_halberd_value / 3) if noxious_halberd_value else provided_value
        else:
            return provided_value
    if item_lower == "araxyte fang":
        amulet_of_rancour_value = await get_most_recent_price_by_name("Amulet of rancour")
        torture_value = await get_most_recent_price_by_name("Amulet of torture")
        if amulet_of_rancour_value and torture_value:
            return amulet_of_rancour_value - torture_value
        else:
            return provided_value
    if item_lower == "mokhaiotl cloth":
        tormented_bracelet_value = await get_most_recent_price_by_name("Tormented bracelet")
        demon_tear_value = await get_most_recent_price_by_name("Demon tear")
        confliction_gauntlet_value = await get_most_recent_price_by_name("Confliction gauntlets")
        if confliction_gauntlet_value and tormented_bracelet_value and demon_tear_value:
            return confliction_gauntlet_value - tormented_bracelet_value - (demon_tear_value * 10000)
        else:
            return 5000000

    else:
        return provided_value

async def get_mapping():
    """Fetch the item mapping data which contains names, IDs, and other metadata"""
    endpoint = f"{PRICES_API_BASE}/mapping"
    session = await get_prices_session()
    async with session.get(endpoint) as resp:
        if resp.status != 200:
            return None
        return await resp.json()

async def find_item_id_by_name(name):
    """Find an item ID by name using the mapping data"""
    mapping_data = await get_mapping()
    if not mapping_data:
        return None
    
    name_lower = name.lower()
    for item in mapping_data:
        if item.get('name', '').lower() == name_lower:
            return item['id']
    return None

async def get_latest_price_data(item_id):
    """Fetch the latest price data from the real-time prices API"""
    endpoint = f"{PRICES_API_BASE}/latest"
    params = {'id': item_id}
    session = await get_prices_session()
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

async def get_most_recent_price_by_id(item_id):
    """
    Get the most recent price for an item by ID
    Returns the price as an integer, or None if not found
    """
    if not item_id:
        return None
    
    price_data = await get_latest_price_data(item_id)
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

async def get_most_recent_price_by_name(item_name):
    """
    Get the most recent price for an item by name
    Returns the price as an integer, or None if not found
    """
    item_id = await find_item_id_by_name(item_name)
    if not item_id:
        return None
    
    return await get_most_recent_price_by_id(item_id)

async def close_aiohttp_sessions():
    global prices_session, wiki_session
    if prices_session is not None and not prices_session.closed:
        await prices_session.close()
    if wiki_session is not None and not wiki_session.closed:
        await wiki_session.close()

