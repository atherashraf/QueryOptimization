import os

from sqlalchemy import create_engine
from sqlalchemy.engine import LegacyCursorResult
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from app.config.config import ConfigUtils

config_utils = ConfigUtils()
db_user = os.getenv("db_user")  # or config_utils.get_data('db_user')
db_password = os.getenv("db_password")  # or config_utils.get_data('db_password')
db_host = os.getenv("db_host")  # or config_utils.get_data('db_host')
db_port = os.getenv("db_port")  # or  config_utils.get_data('db_port')
db_name = os.getenv("db_name")  # or config_utils.get_data('db_name')
SQLALCHEMY_DATABASE_URL = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
print("db url", SQLALCHEMY_DATABASE_URL)

# engine = create_engine(
#     SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False}
# )

engine = create_engine(
    SQLALCHEMY_DATABASE_URL, connect_args={}, pool_size=20, max_overflow=0
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_raw_query_qs(query) -> LegacyCursorResult:
    with engine.connect() as con:
        rs = con.execute(query)
        return rs
