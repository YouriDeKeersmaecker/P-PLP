from sqlalchemy.orm import sessionmaker
from .engine import get_engine

engine = get_engine()

SessionLocal = sessionmaker(
    bind=engine,
    autoflush=False,
    autocommit=False,
    future=True
)

def get_session():
    return SessionLocal()
