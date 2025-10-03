"""
OSRS API Package

A unified package for the DropTracker's interactions with Old School RuneScape Wiki's APIs including:
- OSRS Wiki Bucket API (for item/monster data and drop verification/information)
- RuneScape Wiki Prices API (for Grand Exchange pricing)

This package provides a clean interface for common operations like:
- Checking if an item drops from a specific NPC
- Getting item and NPC IDs
- Retrieving Grand Exchange prices
- Getting combat achievement tier information
"""

from .client import OSRSAPIClient
from .semantic import SemanticAPI
from .pricing import PricingAPI

__version__ = "1.0.0"
__all__ = ["OSRSAPIClient", "SemanticAPI", "PricingAPI"]

# Convenience function to create a fully configured client
def create_client(user_agent: str = "@joelhalen - www.droptracker.io") -> OSRSAPIClient:
    """
    Create a fully configured OSRS API client with all sub-APIs initialized.
    
    Args:
        user_agent: User agent string for API requests
        
    Returns:
        Configured OSRSAPIClient instance
    """
    return OSRSAPIClient(user_agent)
