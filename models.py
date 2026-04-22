"""Database models and configuration for movie cron jobs."""

import os

from sqlalchemy import JSON, Boolean, Column, DateTime, Float, ForeignKey, Integer, String, UniqueConstraint, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    pghost = os.getenv("PGHOST", "localhost")
    pgport = os.getenv("PGPORT", "5432")
    pguser = os.getenv("PGUSER", "postgres")
    pgpassword = os.getenv("PGPASSWORD", "")
    pgdatabase = os.getenv("PGDATABASE", "railway")

    if pgpassword:
        DATABASE_URL = f"postgresql://{pguser}:{pgpassword}@{pghost}:{pgport}/{pgdatabase}"
    else:
        DATABASE_URL = "sqlite:///./movies_scheduler.db"

if DATABASE_URL.startswith("postgresql"):
    print("🐘 Using PostgreSQL database from Railway")
    engine = create_engine(DATABASE_URL)
elif DATABASE_URL.startswith("sqlite"):
    print("🗄️ Using SQLite database for local development")
    engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
else:
    print(f"🤔 Using unknown database: {DATABASE_URL}")
    engine = create_engine(DATABASE_URL)

Base = declarative_base()
SessionLocal = sessionmaker(bind=engine)


def init_db() -> None:
    Base.metadata.create_all(bind=engine)


class MovieLetterboxdData(Base):
    __tablename__ = "movie_letterboxd_data"
    __table_args__ = (
        UniqueConstraint("normalized_title", "year", name="uq_movie_letterboxd_title_year"),
    )

    id = Column(Integer, primary_key=True, index=True)
    title = Column(String, nullable=False)
    normalized_title = Column(String, nullable=False, index=True)
    year = Column(Integer, nullable=True, index=True)
    letterboxd_rating = Column(Float, nullable=True)
    on_watchlist = Column(Boolean, default=False, nullable=False)
    watched = Column(Boolean, default=False, nullable=False)
    personal_rating = Column(Float, nullable=True)
    last_scanned_at = Column(DateTime, nullable=True)

    friend_ratings = relationship(
        "MovieFriendRating",
        back_populates="movie",
        cascade="all, delete-orphan",
    )


class MovieFriendRating(Base):
    __tablename__ = "movie_friend_ratings"
    __table_args__ = (
        UniqueConstraint("movie_id", "friend_username", name="uq_movie_friend_username"),
    )

    id = Column(Integer, primary_key=True, index=True)
    movie_id = Column(Integer, ForeignKey("movie_letterboxd_data.id", ondelete="CASCADE"), nullable=False, index=True)
    friend_username = Column(String, nullable=False)
    friend_display_name = Column(String, nullable=True)
    rating = Column(Float, nullable=True)

    movie = relationship("MovieLetterboxdData", back_populates="friend_ratings")


class MovieScheduleSnapshot(Base):
    __tablename__ = "movie_schedule_snapshots"

    id = Column(Integer, primary_key=True, index=True)
    snapshot_key = Column(String, nullable=False, unique=True, index=True)
    payload = Column(JSON, nullable=False)
    updated_at = Column(DateTime, nullable=False)
