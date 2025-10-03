from .base import Base, session, xenforo_engine, XenforoSession, Session
from .associations import user_group_association
from .user import User
from .npc import NpcList
from .item import ItemList
from .player import Player, IgnoredPlayer
from .group import Group
from .user_configuration import UserConfiguration
from .group_patreon import GroupPatreon
from .drop import Drop
from .collection import CollectionLogEntry
from .combat_achievement import CombatAchievementEntry
from .personal_best import PersonalBestEntry
from .player_pet import PlayerPet
from .group_configuration import GroupConfiguration
from .group_notification import GroupNotification
from .notified_submission import NotifiedSubmission
from .notification_queue import NotificationQueue
from .embed import GroupEmbed, Field
from .guild_meta import Guild, GroupWomAssociation, GroupPersonalBestMessage, LBUpdate
from .webhooks import Webhook, BackupWebhook, WebhookPendingDeletion, NewWebhook
from .lootboard import LootboardStyle
from .analytics import (
    PlayerItemHourlyTotals,
    PlayerNpcHourlyTotals,
    GroupRecentDrops,
    PlayerDailyAggregates,
    PlayerLootData,
    PlayerExperience,
    HistoricalMetrics,
    Log,
)
from datetime import datetime
from .premium_features import ( 
    PremiumFeature, 
    FeatureActivation,
    PointCredit, 
    PointDebit, 
    RecurringPointGrant)
from .tickets import Ticket


def get_current_partition() -> int:
    """
        Returns the naming scheme for a partition of drops
        Based on the current month
    """
    now = datetime.now()
    return now.year * 100 + now.month

__all__ = [
    "Base",
    "session",
    "Session",
    "xenforo_engine",
    "XenforoSession",
    "user_group_association",
    "User",
    "NpcList",
    "ItemList",
    "Player",
    "IgnoredPlayer",
    "Group",
    "UserConfiguration",
    "GroupPatreon",
    "Drop",
    "CollectionLogEntry",
    "CombatAchievementEntry",
    "PersonalBestEntry",
    "PlayerPet",
    "GroupConfiguration",
    "GroupNotification",
    "NotifiedSubmission",
    "NotificationQueue",
    "GroupEmbed",
    "Field",
    "Guild",
    "GroupWomAssociation",
    "GroupPersonalBestMessage",
    "LBUpdate",
    "Webhook",
    "BackupWebhook",
    "WebhookPendingDeletion",
    "NewWebhook",
    "PlayerItemHourlyTotals",
    "PlayerNpcHourlyTotals",
    "GroupRecentDrops",
    "PlayerDailyAggregates",
    "PlayerLootData",
    "PlayerExperience",
    "HistoricalMetrics",
    "Log",
    "PremiumFeature",
    "FeatureActivation",
    "PointCredit",
    "PointDebit",
    "RecurringPointGrant",
    "get_current_partition",
    "LootboardStyle",
    "Ticket"
]
