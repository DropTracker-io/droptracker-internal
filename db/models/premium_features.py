from .base import Base
from sqlalchemy import Column, Integer, String, Boolean, DateTime, ForeignKey, Enum, text
from sqlalchemy import func

from sqlalchemy.dialects.mysql import JSON
from sqlalchemy.schema import Index


class PremiumFeature(Base):
    __tablename__ = 'premium_features'
    id = Column(Integer, primary_key=True, autoincrement=True)
    key = Column(String(64), unique=True, index=True, nullable=False)
    name = Column(String(255), nullable=False)
    description = Column(String(1000))
    scope = Column(Enum('player', 'group', 'both'), nullable=False, server_default=text("'both'"))
    cost_points = Column(Integer, nullable=False)              # points per activation/period
    duration_days = Column(Integer, nullable=False)            # entitlement length
    allow_multiple = Column(Boolean, nullable=False, server_default=text('0'))
    active = Column(Boolean, nullable=False, server_default=text('1'))
    date_added = Column(DateTime, default=func.now())
    date_updated = Column(DateTime, onupdate=func.now(), default=func.now())


class FeatureActivation(Base):
    __tablename__ = 'feature_activations'
    id = Column(Integer, primary_key=True, autoincrement=True)
    player_id = Column(Integer, ForeignKey('players.player_id'), nullable=True)
    group_id = Column(Integer, ForeignKey('groups.group_id'), nullable=True)
    feature_id = Column(Integer, ForeignKey('premium_features.id'), nullable=False)
    start_at = Column(DateTime, nullable=False, default=func.now())
    end_at = Column(DateTime, nullable=False)
    auto_renew = Column(Boolean, nullable=False, server_default=text('0'))
    status = Column(Enum('active','expired','cancelled'), nullable=False, server_default=text("'active'"))
    date_added = Column(DateTime, default=func.now())
    date_updated = Column(DateTime, onupdate=func.now(), default=func.now())

    __table_args__ = (
        Index('idx_activation_owner', 'player_id', 'group_id'),
        Index('idx_activation_status', 'status'),
    )


class PointCredit(Base):
    __tablename__ = 'point_credits'
    id = Column(Integer, primary_key=True, autoincrement=True)
    player_id = Column(Integer, ForeignKey('players.player_id'), nullable=True)
    group_id = Column(Integer, ForeignKey('groups.group_id'), nullable=True)
    source = Column(String(255), nullable=False)
    amount = Column(Integer, nullable=False)
    amount_remaining = Column(Integer, nullable=False)
    earned_at = Column(DateTime, nullable=False, default=func.now())
    expires_at = Column(DateTime, nullable=True)
    # For player-owned credits, this may be used by that player for themselves or their groups.
    # For group-owned credits, usage is restricted to that group only.
    status = Column(Enum('active','expired','revoked'), nullable=False, server_default=text("'active'"))
    revoked_at = Column(DateTime, nullable=True)
    revocation_reason = Column(String(255))
    date_added = Column(DateTime, default=func.now())
    date_updated = Column(DateTime, onupdate=func.now(), default=func.now())

    __table_args__ = (
        Index('idx_credit_owner', 'player_id', 'group_id'),
        Index('idx_credit_expires', 'expires_at'),
        Index('idx_credit_status', 'status'),
    )


class PointDebit(Base):
    __tablename__ = 'point_debits'
    id = Column(Integer, primary_key=True, autoincrement=True)
    # The owner that received the feature/benefit (player or group)
    player_id = Column(Integer, ForeignKey('players.player_id'), nullable=True)
    group_id = Column(Integer, ForeignKey('groups.group_id'), nullable=True)
    # Who paid: either the same group (group credits) or a specific player (player credits)
    spent_by_player_id = Column(Integer, ForeignKey('players.player_id'), nullable=True)

    amount = Column(Integer, nullable=False)
    reason = Column(Enum('feature_activation','manual'), nullable=False)
    created_at = Column(DateTime, nullable=False, default=func.now())
    # Optional: record how the debit was allocated across credits (credit_id→amount)
    allocations = Column(JSON, nullable=True)
    feature_activation_id = Column(Integer, ForeignKey('feature_activations.id'), nullable=True)

    __table_args__ = (
        Index('idx_debit_owner', 'player_id', 'group_id', 'created_at'),
    )


# Recurring point grants for subscriptions/nitro/custom monthly credits
class RecurringPointGrant(Base):
    __tablename__ = 'recurring_point_grants'
    id = Column(Integer, primary_key=True, autoincrement=True)
    player_id = Column(Integer, ForeignKey('players.player_id'), nullable=False)
    source = Column(String(255), nullable=False)
    external_ref = Column(String(128), nullable=True)
    amount_per_period = Column(Integer, nullable=False)
    cadence = Column(Enum('monthly'), nullable=False, server_default=text("'monthly'"))
    last_granted_at = Column(DateTime, nullable=True)
    next_due_at = Column(DateTime, nullable=True)
    status = Column(Enum('active','paused','cancelled'), nullable=False, server_default=text("'active'"))
    extra_data = Column(JSON, nullable=True)
    date_added = Column(DateTime, default=func.now())
    date_updated = Column(DateTime, onupdate=func.now(), default=func.now())

    __table_args__ = (
        Index('idx_rpg_player_status_due', 'player_id', 'status', 'next_due_at'),
        Index('idx_rpg_source_ext', 'source', 'external_ref'),
    )