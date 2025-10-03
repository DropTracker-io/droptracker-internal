from sqlalchemy import Column, Integer, String, Text, TIMESTAMP, Enum, ForeignKeyConstraint, Index

from .base import Base


class ForumSections(Base):
    __tablename__ = 'forum_sections'
    __table_args__ = (
        ForeignKeyConstraint(['category_id'], ['forum_categories.category_id'], name='forum_sections_ibfk_1'),
        Index('category_id', 'category_id'),
        {'extend_existing': True},
    )

    section_id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    category_id = Column(Integer)
    description = Column(Text)
    icon = Column(String(50))
    display_order = Column(Integer)
    created_at = Column(TIMESTAMP)


class ForumThreads(Base):
    __tablename__ = 'forum_threads'
    __table_args__ = (
        ForeignKeyConstraint(['section_id'], ['forum_sections.section_id'], name='forum_threads_ibfk_1'),
        ForeignKeyConstraint(['user_id'], ['users.user_id'], name='forum_threads_ibfk_2'),
        Index('section_id', 'section_id'),
        Index('user_id', 'user_id'),
        {'extend_existing': True},
    )

    thread_id = Column(Integer, primary_key=True)
    title = Column(String(200), nullable=False)
    section_id = Column(Integer)
    user_id = Column(Integer)
    content = Column(Text)
    is_pinned = Column(Integer)
    is_locked = Column(Integer)
    view_count = Column(Integer)
    created_at = Column(TIMESTAMP)
    updated_at = Column(TIMESTAMP)


class ForumPosts(Base):
    __tablename__ = 'forum_posts'
    __table_args__ = (
        ForeignKeyConstraint(['thread_id'], ['forum_threads.thread_id'], name='forum_posts_ibfk_1'),
        ForeignKeyConstraint(['user_id'], ['users.user_id'], name='forum_posts_ibfk_2'),
        Index('thread_id', 'thread_id'),
        Index('user_id', 'user_id'),
        {'extend_existing': True},
    )

    post_id = Column(Integer, primary_key=True)
    thread_id = Column(Integer)
    user_id = Column(Integer)
    content = Column(Text)
    created_at = Column(TIMESTAMP)
    updated_at = Column(TIMESTAMP)


class ForumReactions(Base):
    __tablename__ = 'forum_reactions'
    __table_args__ = (
        ForeignKeyConstraint(['post_id'], ['forum_posts.post_id'], name='forum_reactions_ibfk_1'),
        ForeignKeyConstraint(['user_id'], ['users.user_id'], name='forum_reactions_ibfk_2'),
        Index('post_id', 'post_id'),
        Index('user_id', 'user_id'),
        {'extend_existing': True},
    )

    reaction_id = Column(Integer, primary_key=True)
    post_id = Column(Integer)
    user_id = Column(Integer)
    reaction_type = Column(String(20))
    created_at = Column(TIMESTAMP)


class ForumCategories(Base):
    __tablename__ = 'forum_categories'
    __table_args__ = {'extend_existing': True}

    category_id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    description = Column(Text)
    icon = Column(String(50))
    display_order = Column(Integer)
    created_at = Column(TIMESTAMP)


class XfDtCronLogs(Base):
    __tablename__ = 'xf_dt_cron_logs'
    __table_args__ = (
        Index('cron_id', 'cron_id'),
        Index('start_date', 'start_date'),
        Index('status', 'status'),
        {'extend_existing': True},
    )

    log_id = Column(Integer, primary_key=True)
    cron_id = Column(String(50), nullable=False)
    execution_time = Column(Integer)
    start_date = Column(Integer, nullable=False)
    end_date = Column(Integer, nullable=False)
    status = Column(Enum('success', 'error', 'warning', 'info'))
    context = Column(String(100))
    memory_usage = Column(Integer)
    peak_memory_usage = Column(Integer)
    message = Column(Text)
    error_message = Column(Text)


class CronLog(Base):
    __tablename__ = 'cron_log'
    __table_args__ = (
        Index('idx_executed_at', 'executed_at'),
        Index('idx_job_name', 'job_name'),
        {'extend_existing': True},
    )

    id = Column(Integer, primary_key=True)
    job_name = Column(String(255), nullable=False)
    status = Column(Enum('success', 'error'), nullable=False)
    executed_at = Column(TIMESTAMP, nullable=False)
    message = Column(Text)


