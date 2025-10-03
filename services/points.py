## Handles all point-related logic for premium features and etc.

from typing import List, Optional, Dict, Tuple
from datetime import datetime, timedelta

from sqlalchemy import or_, func

from db import (
    session as db_session,
    PremiumFeature,
    FeatureActivation,
    PointCredit,
    PointDebit,
    RecurringPointGrant,
    user_group_association,
    models
)


# ----------------------------
# Awarding credits
# ----------------------------

def award_points_to_player(*, player_id: int, amount: int, source: str = 'ingame', expires_in_days: Optional[int] = None, session=None) -> int:
    """Create a PointCredit for a player (achievement, nitro via user, admin, etc.).

    Returns the new credit id.
    """
    if amount <= 0:
        raise ValueError("amount must be positive")

    own_session = False
    if session is None:
        session = db_session
        own_session = True

    expires_at = None
    if expires_in_days is not None:
        expires_at = datetime.now() + timedelta(days=expires_in_days)

    credit = PointCredit(
        player_id=player_id,
        group_id=None,
        source=source,
        amount=amount,
        amount_remaining=amount,
        expires_at=expires_at,
        status='active'
    )
    session.add(credit)
    session.flush()
    if own_session:
        session.commit()
    return get_player_point_balance(player_id=player_id, session=session)


def award_points_to_group(*, group_id: int, amount: int, source: str = 'admin', expires_in_days: Optional[int] = None, session=None) -> int:
    """Create a PointCredit for a group (subscription, nitro boost mapped to guild, admin)."""
    if amount <= 0:
        raise ValueError("amount must be positive")

    own_session = False
    if session is None:
        session = db_session
        own_session = True

    expires_at = None
    if expires_in_days is not None:
        expires_at = datetime.now() + timedelta(days=expires_in_days)

    credit = PointCredit(
        player_id=None,
        group_id=group_id,
        source=source,
        amount=amount,
        amount_remaining=amount,
        expires_at=expires_at,
        status='active'
    )
    session.add(credit)
    session.flush()
    if own_session:
        session.commit()
    return credit.id


# ----------------------------
# Balances and availability
# ----------------------------

def _active_credit_filter(now: datetime):
    return (PointCredit.status == 'active',
            or_(PointCredit.expires_at.is_(None), PointCredit.expires_at > now),
            PointCredit.amount_remaining > 0)


def get_player_point_balance(*, player_id: int, session=None) -> int:
    """Sum of a player's active, non-expired, remaining points."""
    if session is None:
        session = db_session
    now = datetime.now()
    filters = _active_credit_filter(now)
    total = (session.query(PointCredit)
             .filter(PointCredit.player_id == player_id, *filters)
             .with_entities(PointCredit.amount_remaining)
             .all())
    return int(sum(r[0] for r in total))


def get_group_point_balance(*, group_id: int, session=None) -> int:
    """Sum of a group's active, non-expired, remaining points."""
    if session is None:
        session = db_session
    now = datetime.now()
    filters = _active_credit_filter(now)
    total = (session.query(PointCredit)
             .filter(PointCredit.group_id == group_id, *filters)
             .with_entities(PointCredit.amount_remaining)
             .all())
    return int(sum(r[0] for r in total))


# ----------------------------
# Feature activation helpers
# ----------------------------

def _eligible_credits_for_player(session, *, player_id: int):
    now = datetime.now()
    filters = _active_credit_filter(now)
    return (session.query(PointCredit)
            .filter(PointCredit.player_id == player_id, *filters)
            .order_by(
                PointCredit.expires_at.is_(None),
                PointCredit.expires_at.asc(),
                PointCredit.earned_at.asc(),
                PointCredit.id.asc()
            ))


def _eligible_credits_for_group(session, *, group_id: int):
    now = datetime.now()
    filters = _active_credit_filter(now)
    return (session.query(PointCredit)
            .filter(PointCredit.group_id == group_id, *filters)
            .order_by(
                PointCredit.expires_at.is_(None),
                PointCredit.expires_at.asc(),
                PointCredit.earned_at.asc(),
                PointCredit.id.asc()
            ))


def _eligible_credits_for_group_with_player(session, *, group_id: int, spender_player_id: int):
    now = datetime.now()
    filters = _active_credit_filter(now)
    return (session.query(PointCredit)
            .filter(*filters)
            .filter(or_(PointCredit.group_id == group_id, PointCredit.player_id == spender_player_id))
            .order_by(
                PointCredit.expires_at.is_(None),
                PointCredit.expires_at.asc(),
                PointCredit.earned_at.asc(),
                PointCredit.id.asc()
            ))


def _consume_points(session, credits_query, need: int) -> Tuple[List[Dict], int]:
    """Consume points from credits in FIFO/soonest-expiry order.

    Returns (allocations, total_taken). Raises ValueError if insufficient.
    """
    if need <= 0:
        raise ValueError("required amount must be positive")

    allocations: List[Dict] = []
    remaining = need

    credits = credits_query.with_for_update().all()
    now = datetime.now()
    for c in credits:
        if c.expires_at and c.expires_at <= now:
            c.status = 'expired'
            continue
        if c.amount_remaining <= 0:
            continue
        take = min(c.amount_remaining, remaining)
        if take <= 0:
            continue
        c.amount_remaining -= take
        remaining -= take
        allocations.append({"credit_id": c.id, "amount": take})
        if remaining == 0:
            break

    if remaining > 0:
        raise ValueError("insufficient points")

    return allocations, need


def activate_feature_for_player(*, player_id: int, feature_key: str, auto_renew: bool = False, session=None) -> Dict:
    """Spend player's credits to activate a feature for themselves."""
    if session is None:
        session = db_session

    with session.begin():
        feature: PremiumFeature = (session.query(PremiumFeature)
                                   .filter_by(key=feature_key, active=True)
                                   .one())

        credits_q = _eligible_credits_for_player(session, player_id=player_id)
        allocations, taken = _consume_points(session, credits_q, feature.cost_points)

        debit = PointDebit(
            player_id=player_id,
            group_id=None,
            spent_by_player_id=player_id,
            amount=taken,
            reason='feature_activation',
            allocations=allocations
        )
        session.add(debit)
        session.flush()

        activation = FeatureActivation(
            player_id=player_id,
            group_id=None,
            feature_id=feature.id,
            start_at=datetime.now(),
            end_at=datetime.now() + timedelta(days=feature.duration_days),
            auto_renew=auto_renew,
            status='active'
        )
        session.add(activation)
        session.flush()
        debit.feature_activation_id = activation.id

    return {"activation_id": activation.id, "debit_id": debit.id}


def activate_feature_for_group(*, group_id: int, feature_key: str, spender_player_id: Optional[int] = None, auto_renew: bool = False, session=None) -> Dict:
    """Spend group credits, or a specific player's credits (if member), to activate a feature for a group."""
    if session is None:
        session = db_session

    with session.begin():
        feature: PremiumFeature = (session.query(PremiumFeature)
                                   .filter_by(key=feature_key, active=True)
                                   .one())

        if spender_player_id is None:
            credits_q = _eligible_credits_for_group(session, group_id=group_id)
        else:
            # Validate membership
            is_member = (session.query(user_group_association)
                         .filter(user_group_association.c.group_id == group_id,
                                 user_group_association.c.player_id == spender_player_id)
                         .first())
            if not is_member:
                raise PermissionError("player is not a member of this group")
            credits_q = _eligible_credits_for_group_with_player(session, group_id=group_id, spender_player_id=spender_player_id)

        allocations, taken = _consume_points(session, credits_q, feature.cost_points)

        debit = PointDebit(
            player_id=None,
            group_id=group_id,
            spent_by_player_id=spender_player_id,
            amount=taken,
            reason='feature_activation',
            allocations=allocations
        )
        session.add(debit)
        session.flush()

        activation = FeatureActivation(
            player_id=None,
            group_id=group_id,
            feature_id=feature.id,
            start_at=datetime.now(),
            end_at=datetime.now() + timedelta(days=feature.duration_days),
            auto_renew=auto_renew,
            status='active'
        )
        session.add(activation)
        session.flush()
        debit.feature_activation_id = activation.id

    return {"activation_id": activation.id, "debit_id": debit.id}


# ----------------------------
# Active checks and listings
# ----------------------------

def is_feature_active_for_player(*, player_id: int, feature_key: str, session=None) -> bool:
    if session is None:
        session = db_session
    now = datetime.now()
    q = (session.query(FeatureActivation)
         .join(PremiumFeature, PremiumFeature.id == FeatureActivation.feature_id)
         .filter(FeatureActivation.player_id == player_id,
                 FeatureActivation.status == 'active',
                 FeatureActivation.end_at > now,
                 PremiumFeature.key == feature_key))
    return session.query(q.exists()).scalar()


def is_feature_active_for_group(*, group_id: int, feature_key: str, session=None) -> bool:
    if session is None:
        session = db_session
    now = datetime.now()
    q = (session.query(FeatureActivation)
         .join(PremiumFeature, PremiumFeature.id == FeatureActivation.feature_id)
         .filter(FeatureActivation.group_id == group_id,
                 FeatureActivation.status == 'active',
                 FeatureActivation.end_at > now,
                 PremiumFeature.key == feature_key))
    return session.query(q.exists()).scalar()


def list_active_features_for_player(*, player_id: int, session=None) -> List[Dict]:
    if session is None:
        session = db_session
    now = datetime.now()
    rows = (session.query(FeatureActivation, PremiumFeature)
            .join(PremiumFeature, PremiumFeature.id == FeatureActivation.feature_id)
            .filter(FeatureActivation.player_id == player_id,
                    FeatureActivation.status == 'active',
                    FeatureActivation.end_at > now)
            .all())
    return [{"key": f.key, "name": f.name, "end_at": a.end_at} for a, f in rows]


def list_active_features_for_group(*, group_id: int, session=None) -> List[Dict]:
    if session is None:
        session = db_session
    now = datetime.now()
    rows = (session.query(FeatureActivation, PremiumFeature)
            .join(PremiumFeature, PremiumFeature.id == FeatureActivation.feature_id)
            .filter(FeatureActivation.group_id == group_id,
                    FeatureActivation.status == 'active',
                    FeatureActivation.end_at > now)
            .all())
    return [{"key": f.key, "name": f.name, "end_at": a.end_at} for a, f in rows]


# ----------------------------
# Expiry and revocation
# ----------------------------

def expire_due_credits(*, session=None) -> int:
    """Mark credits with past expires_at as expired. Returns count affected."""
    if session is None:
        session = db_session
    now = datetime.now()
    # Fetch ids first to keep rowcount reliable across backends
    ids = [cid for (cid,) in (session.query(PointCredit.id)
                              .filter(PointCredit.status == 'active',
                                      PointCredit.expires_at.isnot(None),
                                      PointCredit.expires_at <= now)
                              .all())]
    if not ids:
        return 0
    (session.query(PointCredit)
     .filter(PointCredit.id.in_(ids))
     .update({PointCredit.status: 'expired'}, synchronize_session=False))
    session.commit()
    return len(ids)


def revoke_credit(*, credit_id: int, reason: Optional[str] = None, session=None) -> None:
    if session is None:
        session = db_session
    with session.begin():
        c: PointCredit = session.query(PointCredit).filter_by(id=credit_id).with_for_update().one()
        c.amount_remaining = 0
        c.status = 'revoked'
        c.revoked_at = datetime.now()
        c.revocation_reason = reason


# ----------------------------
# Convenience helpers
# ----------------------------

def get_available_points_for_group_spend(*, group_id: int, spender_player_id: Optional[int] = None, session=None) -> int:
    """Total available points that can be used to pay for a group feature.

    Includes group credits; if spender_player_id provided and they are a member, also includes that player's credits.
    """
    if session is None:
        session = db_session
    total = get_group_point_balance(group_id=group_id, session=session)
    if spender_player_id is None:
        return total

    is_member = (session.query(user_group_association)
                 .filter(user_group_association.c.group_id == group_id,
                         user_group_association.c.player_id == spender_player_id)
                 .first())
    if not is_member:
        return total
    return total + get_player_point_balance(player_id=spender_player_id, session=session)


# ----------------------------
# Reporting
# ----------------------------

def get_player_lifetime_points_earned(*, player_id: int, session=None) -> int:
    """Total points ever credited to a player, regardless of expiry or status.

    Excludes credits whose source contains 'Upgrade' (case-insensitive).
    """
    if session is None:
        session = db_session

    total = (session.query(func.sum(PointCredit.amount))
             .filter(PointCredit.player_id == player_id)
             .filter(or_(PointCredit.source.is_(None), ~PointCredit.source.ilike('%Upgrade%')))
             .scalar())
    return int(total or 0)


# ----------------------------
# Recurring point grants (subscriptions, nitro, etc.)
# ----------------------------

def _last_day_of_month(year: int, month: int) -> int:
    if month == 12:
        next_first = datetime(year + 1, 1, 1)
    else:
        next_first = datetime(year, month + 1, 1)
    return (next_first - timedelta(days=1)).day

def _add_months(dt: datetime, months: int = 1) -> datetime:
    m = dt.month - 1 + months
    year = dt.year + m // 12
    month = m % 12 + 1
    day = min(dt.day, _last_day_of_month(year, month))
    return dt.replace(year=year, month=month, day=day)

def _find_rpg(session, *, player_id: int, source: str, external_ref: Optional[str]):
    q = session.query(RecurringPointGrant).filter(
        RecurringPointGrant.player_id == player_id,
        RecurringPointGrant.source == source,
    )
    if external_ref is None:
        q = q.filter(RecurringPointGrant.external_ref.is_(None))
    else:
        q = q.filter(RecurringPointGrant.external_ref == external_ref)
    return q.first()

def ensure_recurring_grant_for_player(
    *,
    player_id: int,
    source: str,                    # 'subscription' | 'nitro' | 'custom'
    amount_per_month: int,
    external_ref: Optional[str] = None,
    extra_data: Optional[Dict] = None,
    start_immediately: bool = True,
    grant_on_upgrade: bool = True,
    session=None
) -> int:
    """Create or update a recurring grant record for a player.

    - Start immediately (grant due now) by default.
    - If the amount increases (upgrade), schedule the next grant now when grant_on_upgrade is True.
    """
    if source not in ('subscription', 'nitro', 'custom'):
        raise ValueError("invalid source for recurring grant")
    if amount_per_month <= 0:
        raise ValueError("amount_per_month must be positive")

    own_session = False
    if session is None:
        session = db_session
        own_session = True

    now = datetime.now()
    with session.begin():
        rpg = _find_rpg(session, player_id=player_id, source=source, external_ref=external_ref)
        if rpg is None:
            rpg = RecurringPointGrant(
                player_id=player_id,
                source=source,
                external_ref=external_ref,
                amount_per_period=amount_per_month,
                cadence='monthly',
                status='active',
                last_granted_at=None,
                next_due_at=(now if start_immediately else _add_months(now, 1)),
                extra_data=extra_data or None,
            )
            session.add(rpg)
            session.flush()
            return rpg.id

        # Update existing
        upgraded = amount_per_month > rpg.amount_per_period
        rpg.amount_per_period = amount_per_month
        if rpg.status != 'active':
            rpg.status = 'active'
            if rpg.next_due_at is None or rpg.next_due_at > now:
                rpg.next_due_at = now

        if upgraded and grant_on_upgrade:
            # Force a grant on the next processor run
            rpg.next_due_at = now

        session.flush()
        return rpg.id

def cancel_recurring_grant_for_player(
    *,
    player_id: int,
    source: str,
    external_ref: Optional[str] = None,
    session=None
) -> bool:
    """Cancel a recurring grant; returns True if found."""
    own_session = False
    if session is None:
        session = db_session
        own_session = True

    with session.begin():
        rpg = _find_rpg(session, player_id=player_id, source=source, external_ref=external_ref)
        if not rpg:
            return False
        rpg.status = 'cancelled'
        rpg.next_due_at = None
        return True

def process_recurring_point_grants(*, batch_size: int = 100, session=None) -> int:
    """Grant points for all due recurring grants.

    Returns the number of grants processed.
    """
    own_session = False
    if session is None:
        session = db_session
        own_session = True

    now = datetime.now()
    processed = 0

    with session.begin():
        # Fetch due grants in a deterministic order
        due = (session.query(RecurringPointGrant)
               .filter(RecurringPointGrant.status == 'active',
                       RecurringPointGrant.cadence == 'monthly',
                       RecurringPointGrant.next_due_at.isnot(None),
                       RecurringPointGrant.next_due_at <= now)
               .order_by(RecurringPointGrant.next_due_at.asc(), RecurringPointGrant.id.asc())
               .limit(batch_size)
               .all())

        for rpg in due:
            try:
                # Issue credit
                award_points_to_player(
                    player_id=rpg.player_id,
                    amount=rpg.amount_per_period,
                    source=(rpg.source if rpg.source in ('subscription', 'nitro') else 'admin'),
                    session=session,
                )
                # Advance schedule
                rpg.last_granted_at = now
                rpg.next_due_at = _add_months(now, 1)
                processed += 1
            except Exception:
                # Continue with other grants
                continue

    return processed

# Convenience wrappers for common sources

def upsert_patreon_grant_for_player(
    *,
    player_id: int,
    pledge_id: str,
    tier_amount_points_per_month: int,
    extra_data: Optional[Dict] = None,
    start_immediately: bool = True,
    session=None
) -> int:
    """Create/update a Patreon-based recurring grant keyed by pledge_id."""
    return ensure_recurring_grant_for_player(
        player_id=player_id,
        source='subscription',
        amount_per_month=tier_amount_points_per_month,
        external_ref=pledge_id,
        extra_data=extra_data,
        start_immediately=start_immediately,
        grant_on_upgrade=True,
        session=session,
    )

def upsert_discord_nitro_grant_for_player(
    *,
    player_id: int,
    discord_user_id: str,
    amount_points_per_month: int,
    active: bool,
    session=None
) -> Optional[int]:
    """Create/update/cancel a Nitro-based recurring grant keyed by Discord user id."""
    if active:
        return ensure_recurring_grant_for_player(
            player_id=player_id,
            source='nitro',
            amount_per_month=amount_points_per_month,
            external_ref=discord_user_id,
            extra_data=None,
            start_immediately=True,
            grant_on_upgrade=False,
            session=session,
        )
    else:
        cancel_recurring_grant_for_player(
            player_id=player_id,
            source='nitro',
            external_ref=discord_user_id,
            session=session,
        )
        return None