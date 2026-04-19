#!/usr/bin/env python3

import datetime
import json
import sqlalchemy
from sqlalchemy import (
    Table,
    Column,
    Integer,
    String,
    Float,
    Boolean,
    DateTime,
    MetaData,
    insert,
    select,
    update,
    delete,
    and_,
    or_,
    func,
)
from typing import Any


class UnifiedTrackCache:
    """
    SQLite-backed unified cache for tracks from both Tidal and Spotify.
    Stores track metadata and match information to avoid repeated API searches.
    """

    def __init__(self, filename=".tracks.db"):
        self.engine = sqlalchemy.create_engine(f"sqlite:///{filename}")
        meta = MetaData()

        self.tracks = Table(
            "tracks",
            meta,
            Column("id", Integer, primary_key=True),
            Column("tidal_id", String, unique=True, nullable=True),
            Column("tidal_name", String, nullable=True),
            Column("tidal_artists", String, nullable=True),
            Column("tidal_album", String, nullable=True),
            Column("tidal_isrc", String, nullable=True),
            Column("tidal_duration", Integer, nullable=True),
            Column("tidal_track_num", Integer, nullable=True),
            Column("spotify_id", String, unique=True, nullable=True),
            Column("spotify_name", String, nullable=True),
            Column("spotify_artists", String, nullable=True),
            Column("spotify_album", String, nullable=True),
            Column("spotify_isrc", String, nullable=True),
            Column("spotify_duration", Integer, nullable=True),
            Column("match_confidence", Float, nullable=True),
            Column("match_method", String, nullable=True),
            Column("matched_at", DateTime, nullable=True),
            Column("created_at", DateTime, default=datetime.datetime.now),
            Column(
                "updated_at",
                DateTime,
                default=datetime.datetime.now,
                onupdate=datetime.datetime.now,
            ),
        )

        self.sync_history = Table(
            "sync_history",
            meta,
            Column("id", Integer, primary_key=True),
            Column("direction", String),
            Column("track_count", Integer),
            Column("synced_at", DateTime, default=datetime.datetime.now),
            Column("dry_run", Boolean),
        )

        meta.create_all(self.engine)

    def store_tidal_track(
        self,
        tidal_id: str,
        name: str,
        artists: list[str],
        album: str | None,
        isrc: str | None,
        duration: int | None,
        track_num: int | None = None,
    ):
        """Store or update Tidal track metadata"""
        with self.engine.connect() as conn:
            with conn.begin():
                existing = conn.execute(
                    select(self.tracks).where(self.tracks.c.tidal_id == tidal_id)
                ).fetchone()

                tidal_artists_json = json.dumps(artists)

                if existing:
                    conn.execute(
                        update(self.tracks)
                        .where(self.tracks.c.tidal_id == tidal_id)
                        .values(
                            tidal_name=name,
                            tidal_artists=tidal_artists_json,
                            tidal_album=album,
                            tidal_isrc=isrc,
                            tidal_duration=duration,
                            tidal_track_num=track_num,
                            updated_at=datetime.datetime.now(),
                        )
                    )
                else:
                    conn.execute(
                        insert(self.tracks),
                        {
                            "tidal_id": tidal_id,
                            "tidal_name": name,
                            "tidal_artists": tidal_artists_json,
                            "tidal_album": album,
                            "tidal_isrc": isrc,
                            "tidal_duration": duration,
                            "tidal_track_num": track_num,
                            "created_at": datetime.datetime.now(),
                            "updated_at": datetime.datetime.now(),
                        },
                    )

    def store_spotify_track(
        self,
        spotify_id: str,
        name: str,
        artists: list[str],
        album: str | None,
        isrc: str | None,
        duration: int | None,
    ):
        """Store or update Spotify track metadata"""
        with self.engine.connect() as conn:
            with conn.begin():
                existing = conn.execute(
                    select(self.tracks).where(self.tracks.c.spotify_id == spotify_id)
                ).fetchone()

                spotify_artists_json = json.dumps(artists)

                if existing:
                    conn.execute(
                        update(self.tracks)
                        .where(self.tracks.c.spotify_id == spotify_id)
                        .values(
                            spotify_name=name,
                            spotify_artists=spotify_artists_json,
                            spotify_album=album,
                            spotify_isrc=isrc,
                            spotify_duration=duration,
                            updated_at=datetime.datetime.now(),
                        )
                    )
                else:
                    conn.execute(
                        insert(self.tracks),
                        {
                            "spotify_id": spotify_id,
                            "spotify_name": name,
                            "spotify_artists": spotify_artists_json,
                            "spotify_album": album,
                            "spotify_isrc": isrc,
                            "spotify_duration": duration,
                            "created_at": datetime.datetime.now(),
                            "updated_at": datetime.datetime.now(),
                        },
                    )

    def store_match(
        self, tidal_id: str, spotify_id: str, confidence: float, method: str
    ):
        """Store a Tidal <-> Spotify match"""
        with self.engine.connect() as conn:
            with conn.begin():
                conn.execute(
                    update(self.tracks)
                    .where(self.tracks.c.tidal_id == tidal_id)
                    .values(
                        spotify_id=spotify_id,
                        match_confidence=confidence,
                        match_method=method,
                        matched_at=datetime.datetime.now(),
                        updated_at=datetime.datetime.now(),
                    )
                )

    def get_by_tidal_id(self, tidal_id: str) -> dict | None:
        """Get track data by Tidal ID"""
        with self.engine.connect() as conn:
            row = conn.execute(
                select(self.tracks).where(self.tracks.c.tidal_id == tidal_id)
            ).fetchone()
            return self._row_to_dict(row) if row else None

    def get_by_spotify_id(self, spotify_id: str) -> dict | None:
        """Get track data by Spotify ID"""
        with self.engine.connect() as conn:
            row = conn.execute(
                select(self.tracks).where(self.tracks.c.spotify_id == spotify_id)
            ).fetchone()
            return self._row_to_dict(row) if row else None

    def get_by_isrc(self, isrc: str, platform: str) -> dict | None:
        """Get track by ISRC from either platform"""
        if platform == "tidal":
            column = self.tracks.c.tidal_isrc
        else:
            column = self.tracks.c.spotify_isrc

        with self.engine.connect() as conn:
            row = conn.execute(select(self.tracks).where(column == isrc)).fetchone()
            return self._row_to_dict(row) if row else None

    def find_tidal_to_spotify_match(
        self,
        tidal_isrc: str | None = None,
        tidal_name: str | None = None,
        tidal_artists: list[str] | None = None,
        tidal_duration: int | None = None,
    ) -> dict | None:
        """Find existing Spotify match for Tidal track data"""
        with self.engine.connect() as conn:
            query = select(self.tracks).where(self.tracks.c.spotify_id.isnot(None))

            if tidal_isrc:
                isrc_match = conn.execute(
                    select(self.tracks).where(
                        and_(
                            self.tracks.c.tidal_isrc == tidal_isrc,
                            self.tracks.c.spotify_id.isnot(None),
                        )
                    )
                ).fetchone()
                if isrc_match:
                    return self._row_to_dict(isrc_match)

            if tidal_name and tidal_artists and tidal_duration:
                candidates = conn.execute(
                    select(self.tracks).where(
                        and_(
                            self.tracks.c.spotify_id.isnot(None),
                            self.tracks.c.spotify_name.isnot(None),
                        )
                    )
                ).fetchall()

                best_match = None
                best_confidence = 0.0

                for row in candidates:
                    confidence = self._calculate_match_confidence(
                        tidal_name,
                        tidal_artists,
                        tidal_duration,
                        row.spotify_name,
                        json.loads(row.spotify_artists) if row.spotify_artists else [],
                        row.spotify_duration,
                    )
                    if confidence > best_confidence and confidence > 0.7:
                        best_match = row
                        best_confidence = confidence

                return self._row_to_dict(best_match) if best_match else None

            return None

    def find_spotify_to_tidal_match(
        self,
        spotify_isrc: str | None = None,
        spotify_name: str | None = None,
        spotify_artists: list[str] | None = None,
        spotify_duration: int | None = None,
    ) -> dict | None:
        """Find existing Tidal match for Spotify track data"""
        with self.engine.connect() as conn:
            if spotify_isrc:
                isrc_match = conn.execute(
                    select(self.tracks).where(
                        and_(
                            self.tracks.c.spotify_isrc == spotify_isrc,
                            self.tracks.c.tidal_id.isnot(None),
                        )
                    )
                ).fetchone()
                if isrc_match:
                    return self._row_to_dict(isrc_match)

            if spotify_name and spotify_artists and spotify_duration:
                candidates = conn.execute(
                    select(self.tracks).where(
                        and_(
                            self.tracks.c.tidal_id.isnot(None),
                            self.tracks.c.tidal_name.isnot(None),
                        )
                    )
                ).fetchall()

                best_match = None
                best_confidence = 0.0

                for row in candidates:
                    confidence = self._calculate_match_confidence(
                        spotify_name,
                        spotify_artists,
                        spotify_duration,
                        row.tidal_name,
                        json.loads(row.tidal_artists) if row.tidal_artists else [],
                        row.tidal_duration,
                    )
                    if confidence > best_confidence and confidence > 0.7:
                        best_match = row
                        best_confidence = confidence

                return self._row_to_dict(best_match) if best_match else None

            return None

    def get_unmatched_tidal(self, limit: int | None = None) -> list[dict]:
        """Get all Tidal tracks without Spotify match"""
        with self.engine.connect() as conn:
            query = select(self.tracks).where(
                and_(
                    self.tracks.c.tidal_id.isnot(None),
                    self.tracks.c.spotify_id.is_(None),
                )
            )
            if limit:
                query = query.limit(limit)
            rows = conn.execute(query).fetchall()
            return [self._row_to_dict(row) for row in rows]

    def get_unmatched_spotify(self, limit: int | None = None) -> list[dict]:
        """Get all Spotify tracks without Tidal match"""
        with self.engine.connect() as conn:
            query = select(self.tracks).where(
                and_(
                    self.tracks.c.spotify_id.isnot(None),
                    self.tracks.c.tidal_id.is_(None),
                )
            )
            if limit:
                query = query.limit(limit)
            rows = conn.execute(query).fetchall()
            return [self._row_to_dict(row) for row in rows]

    def get_not_found_list(self) -> list[dict]:
        """Get tracks that were searched but not found on the other platform"""
        with self.engine.connect() as conn:
            rows = conn.execute(
                select(self.tracks).where(
                    and_(
                        or_(
                            self.tracks.c.tidal_id.isnot(None),
                            self.tracks.c.spotify_id.isnot(None),
                        ),
                        self.tracks.c.match_confidence.isnot(None),
                        self.tracks.c.matched_at.is_(None),
                    )
                )
            ).fetchall()
            return [self._row_to_dict(row) for row in rows]

    def add_sync_history(self, direction: str, track_count: int, dry_run: bool):
        """Record a sync operation in history"""
        with self.engine.connect() as conn:
            with conn.begin():
                conn.execute(
                    insert(self.sync_history),
                    {
                        "direction": direction,
                        "track_count": track_count,
                        "dry_run": dry_run,
                        "synced_at": datetime.datetime.now(),
                    },
                )

    def get_stats(self) -> dict:
        """Get cache statistics"""
        with self.engine.connect() as conn:
            total = conn.execute(select(func.count(self.tracks.c.id))).scalar()
            matched = conn.execute(
                select(func.count(self.tracks.c.id)).where(
                    and_(
                        self.tracks.c.tidal_id.isnot(None),
                        self.tracks.c.spotify_id.isnot(None),
                    )
                )
            ).scalar()
            tidal_only = conn.execute(
                select(func.count(self.tracks.c.id)).where(
                    and_(
                        self.tracks.c.tidal_id.isnot(None),
                        self.tracks.c.spotify_id.is_(None),
                    )
                )
            ).scalar()
            spotify_only = conn.execute(
                select(sqlalchemy.func.count(self.tracks.c.id)).where(
                    and_(
                        self.tracks.c.spotify_id.isnot(None),
                        self.tracks.c.tidal_id.is_(None),
                    )
                )
            ).scalar()

            return {
                "total_tracks": total or 0,
                "matched": matched or 0,
                "tidal_only": tidal_only or 0,
                "spotify_only": spotify_only or 0,
            }

    def _row_to_dict(self, row) -> dict | None:
        """Convert SQLAlchemy row to dict"""
        if row is None:
            return None
        d = dict(row._mapping)
        for key in ["tidal_artists", "spotify_artists"]:
            if key in d and d[key]:
                try:
                    d[key] = json.loads(d[key])
                except (json.JSONDecodeError, TypeError):
                    d[key] = []
            elif key in d:
                d[key] = []
        return d

    def _calculate_match_confidence(
        self,
        name1: str,
        artists1: list[str],
        duration1: int | None,
        name2: str,
        artists2: list[str],
        duration2: int | None,
    ) -> float:
        """Calculate match confidence between two tracks"""
        if not name1 or not name2 or not duration1 or not duration2:
            return 0.0

        confidence = 0.0

        name1_simple = self._simple_name(name1)
        name2_simple = self._simple_name(name2)
        if name1_simple == name2_simple:
            confidence += 0.4

        artists1_set = set(self._simple_artist(a) for a in artists1)
        artists2_set = set(self._simple_artist(a) for a in artists2)
        if artists1_set.intersection(artists2_set):
            confidence += 0.3

        duration_diff = abs(duration1 - duration2)
        if duration_diff < 2:
            confidence += 0.3
        elif duration_diff < 5:
            confidence += 0.15

        return min(confidence, 1.0)

    def _simple_name(self, name: str) -> str:
        """Simplify track name for comparison"""
        return (
            name.lower()
            .split("-")[0]
            .strip()
            .split("(")[0]
            .strip()
            .split("[")[0]
            .strip()
        )

    def _simple_artist(self, artist: str) -> str:
        """Simplify artist name for comparison"""
        return artist.lower().split("&")[0].strip().split(",")[0].strip()


unified_cache = UnifiedTrackCache()
