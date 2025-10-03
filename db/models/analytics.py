from sqlalchemy import Column, Integer, String, Date, DateTime, BigInteger, UniqueConstraint, Index, ForeignKey, Text, Enum, TIMESTAMP
from sqlalchemy import func, text

from .base import Base


class PlayerItemHourlyTotals(Base):
    __tablename__ = 'player_item_hourly_totals'
    __table_args__ = (
        UniqueConstraint('player_id', 'item_id', 'date_hour', 'partition', name='uq_player_item_hourly'),
        Index('idx_player_date_hour', 'player_id', 'date_hour'),
        Index('idx_item_date_hour', 'item_id', 'date_hour'),
        Index('idx_partition_date_hour', 'partition', 'date_hour'),
        {'extend_existing': True},
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    player_id = Column(Integer, ForeignKey('players.player_id'), nullable=False)
    item_id = Column(Integer, ForeignKey('items.item_id'), nullable=False)
    date_hour = Column(String(13), nullable=False)
    partition = Column(Integer, nullable=False)
    quantity = Column(Integer, default=0)
    total_value = Column(BigInteger, default=0)
    drop_count = Column(Integer, default=0)
    last_drop_time = Column(DateTime)


class PlayerNpcHourlyTotals(Base):
    __tablename__ = 'player_npc_hourly_totals'
    __table_args__ = (
        UniqueConstraint('player_id', 'npc_id', 'date_hour', 'partition', name='uq_player_npc_hourly'),
        Index('idx_player_npc_date_hour', 'player_id', 'npc_id', 'date_hour'),
        Index('idx_npc_date_hour', 'npc_id', 'date_hour'),
        Index('idx_partition_date_hour', 'partition', 'date_hour'),
        {'extend_existing': True},
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    player_id = Column(Integer, ForeignKey('players.player_id'), nullable=False)
    npc_id = Column(Integer, ForeignKey('npc_list.npc_id'), nullable=False)
    date_hour = Column(String(13), nullable=False)
    partition = Column(Integer, nullable=False)
    total_value = Column(BigInteger, default=0)
    drop_count = Column(Integer, default=0)
    last_drop_time = Column(DateTime)


class GroupRecentDrops(Base):
    __tablename__ = 'group_recent_drops'
    __table_args__ = (
        Index('idx_group_date', 'group_id', 'date_added'),
        Index('idx_group_partition', 'group_id', 'partition'),
        Index('idx_player_date', 'player_id', 'date_added'),
        Index('idx_date_added', 'date_added'),
        {'extend_existing': True},
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    group_id = Column(Integer, ForeignKey('groups.group_id'), nullable=False)
    drop_id = Column(Integer, ForeignKey('drops.drop_id'), nullable=False)
    player_id = Column(Integer, ForeignKey('players.player_id'), nullable=False)
    item_id = Column(Integer, ForeignKey('items.item_id'), nullable=False)
    quantity = Column(Integer, nullable=False)
    value = Column(Integer, nullable=False)
    date_added = Column(DateTime, nullable=False)
    npc_id = Column(Integer, ForeignKey('npc_list.npc_id'), nullable=True)
    partition = Column(Integer, nullable=False)


class PlayerDailyAggregates(Base):
    __tablename__ = 'player_daily_aggregates'
    __table_args__ = (
        UniqueConstraint('player_id', 'date', 'partition', name='uq_player_daily_agg'),
        Index('idx_player_date', 'player_id', 'date'),
        Index('idx_partition_date', 'partition', 'date'),
        {'extend_existing': True},
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    player_id = Column(Integer, ForeignKey('players.player_id'), nullable=False)
    date = Column(Date, nullable=False)
    partition = Column(Integer, nullable=False)
    total_value = Column(BigInteger, default=0)
    drop_count = Column(Integer, default=0)
    unique_items = Column(Integer, default=0)
    unique_npcs = Column(Integer, default=0)
    last_drop_time = Column(DateTime)


class PlayerLootData(Base):
    __tablename__ = 'player_loot_data'
    __table_args__ = {
        'extend_existing': True,
    }

    id = Column(Integer, primary_key=True, autoincrement=True)
    player_id = Column(Integer, nullable=False)
    npc_id = Column(Integer, nullable=True)
    date_updated = Column(DateTime, default=func.now())
    data = Column(Text)
    time_period = Column(Integer, nullable=False)


class PlayerExperience(Base):
    __tablename__ = 'player_exp'
    __table_args__ = {
        'extend_existing': True,
    }

    id = Column(Integer, primary_key=True, autoincrement=True)
    player_id = Column(Integer, ForeignKey('players.player_id'), nullable=False)
    attack = Column(Integer, default=0, nullable=False)
    strength = Column(Integer, default=0, nullable=False)
    defence = Column(Integer, default=0, nullable=False)
    ranged = Column(Integer, default=0, nullable=False)
    prayer = Column(Integer, default=0, nullable=False)
    magic = Column(Integer, default=0, nullable=False)
    runecraft = Column(Integer, default=0, nullable=False)
    hitpoints = Column(Integer, default=0, nullable=False)
    crafting = Column(Integer, default=0, nullable=False)
    mining = Column(Integer, default=0, nullable=False)
    smithing = Column(Integer, default=0, nullable=False)
    woodcutting = Column(Integer, default=0, nullable=False)
    farming = Column(Integer, default=0, nullable=False)
    firemaking = Column(Integer, default=0, nullable=False)
    fishing = Column(Integer, default=0, nullable=False)
    hunter = Column(Integer, default=0, nullable=False)
    herblore = Column(Integer, default=0, nullable=False)
    cooking = Column(Integer, default=0, nullable=False)
    thieving = Column(Integer, default=0, nullable=False)
    construction = Column(Integer, default=0, nullable=False)
    slayer = Column(Integer, default=0, nullable=False)
    agility = Column(Integer, default=0, nullable=False)
    fletching = Column(Integer, default=0, nullable=False)
    sailing = Column(Integer, default=0, nullable=False)
    last_updated = Column(DateTime, default=func.now(), nullable=False)


class HistoricalMetrics(Base):
    __tablename__ = 'historical_metrics'
    __table_args__ = (
        Index('idx_type_timestamp', 'metric_type', 'timestamp'),
        {'extend_existing': True},
    )

    id = Column(Integer, primary_key=True)
    metric_type = Column(String(50), nullable=False)
    value = Column(Integer, nullable=False)
    timestamp = Column(TIMESTAMP, server_default=text('current_timestamp()'))


class Log(Base):
    __tablename__ = 'logs'
    __table_args__ = {
        'extend_existing': True,
    }

    id = Column(Integer, primary_key=True)
    level = Column(String(10), nullable=False)
    source = Column(String(50), nullable=False)
    message = Column(Text, nullable=False)
    details = Column(Text, nullable=True)
    timestamp = Column(BigInteger, index=True)


