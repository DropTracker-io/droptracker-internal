from sqlalchemy import Table, Column, Integer, ForeignKey, UniqueConstraint

from .base import Base


user_group_association = Table(
    'user_group_association', Base.metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('player_id', Integer, ForeignKey('players.player_id'), nullable=True),
    Column('user_id', Integer, ForeignKey('users.user_id'), nullable=True),
    Column('group_id', Integer, ForeignKey('groups.group_id'), nullable=False),
    UniqueConstraint('player_id', 'user_id', 'group_id', name='uq_user_group_player')
)


