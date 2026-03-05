from sqlalchemy import Column, Integer, String, JSON, DateTime, Boolean
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import create_engine
import datetime

DATABASE_URL = "sqlite:///./mapping_configs.db"

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class ProjectMapping(Base):
    __tablename__ = "project_mappings"

    id            = Column(Integer, primary_key=True, index=True)
    project_name  = Column(String, unique=True, index=True)
    version       = Column(String, default="1.0")

    # Replaces JSON files – stores detection rules, column map and derived fields
    detection_config  = Column(JSON)   # {required_headers: [...], optional_headers: [...]}
    column_mappings   = Column(JSON)   # {raw_col: standard_col}
    derived_fields    = Column(JSON)   # {standard_col: {type, source/value}}

    is_active  = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)


def init_db():
    Base.metadata.create_all(bind=engine)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
