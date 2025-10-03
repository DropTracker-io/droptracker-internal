from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session
import os
from dotenv import load_dotenv
import pymysql
#from sqlalchemy.orm import relationship

pymysql.install_as_MySQLdb()
load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

# Create base class for declarative models
Base = declarative_base()

# Create engine with improved connection handling
engine = create_engine(f'mysql+pymysql://{DB_USER}:{DB_PASS}@localhost:3306/data', 
                      pool_size=20, 
                      max_overflow=10, 
                      pool_pre_ping=True,  # Test connections before use
                      pool_recycle=3600,   # Recycle connections every hour
                      connect_args={
                          'connect_timeout': 10,    # Connection timeout
                          'read_timeout': 30,       # Read timeout  
                          'write_timeout': 30,      # Write timeout
                          'charset': 'utf8mb4',
                          'autocommit': False
                      })

# Create session factory and scoped session (hot-swappable parity with legacy)
Session = sessionmaker(bind=engine)
session = scoped_session(Session)

# Secondary XenForo connection (parity with legacy models)
xenforo_engine = create_engine(
    f"mysql+pymysql://{DB_USER}:{DB_PASS}@localhost:3306/xenforo",
    pool_size=20,
    max_overflow=10,
    pool_pre_ping=True,
    pool_timeout=30,
    pool_recycle=3600,
    isolation_level="READ COMMITTED",
)

XenforoSession = sessionmaker(bind=xenforo_engine)

def get_fresh_session():
    return Session()

def get_fresh_xenforo_session():
    return XenforoSession()

# This will be called after all models are defined
def setup_relationships():
    pass
#     """
#     Set up relationships between models after all models are defined.
#     This avoids circular import issues.
#     """
#     from db import Group
#     #from events.models import EventModel
    
#     # Add relationships
#     Group.events = relationship("EventModel", back_populates="group")
#     #EventModel.group = relationship("Group", back_populates="events") 