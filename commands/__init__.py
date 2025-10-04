"""
Commands Package

This package contains all Discord slash commands for the DropTracker bot,
organized by permission level and functionality for better maintainability.

Modules:
    user: User-level commands (help, accounts, settings, etc.)
    admin: Administrator commands (group management, webhooks, etc.)
    utils: Utility functions and helpers for commands
    
Classes:
    UserCommands: Extension containing user-level commands
    ClanCommands: Extension containing clan/admin commands

Author: joelhalen
"""

from .user import UserCommands
from .admin import ClanCommands
from .utils import try_create_user, is_admin, is_user_authorized, get_external_latency

__all__ = [
    'UserCommands',
    'ClanCommands', 
    'try_create_user',
    'is_admin',
    'is_user_authorized',
    'get_external_latency'
]
