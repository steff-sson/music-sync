#!/usr/bin/env python3

import datetime
import json
import os
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

    def __init__(self, filename=".tracks.db", log_dir="."):
        self.engine = sqlalchemy.create_engine(f"sqlite:///{filename}")
        self.log_dir = log_dir
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

        self.not_found = Table(
            "not_found",
            meta,
            Column("id", Integer, primary_key=True),
            Column("direction", String),
            Column("track_id", String),
            Column("track_name", String),
            Column("track_artists", String),
            Column("isrc", String, nullable=True),
            Column("searched_at", DateTime, default=datetime.datetime.now),
        )

        self.artist_genres = Table(
            "artist_genres",
            meta,
            Column("id", Integer, primary_key=True),
            Column("artist_id", String, unique=True),
            Column("artist_name", String),
            Column("genre_category", String),
            Column("genre_source", String, default="spotify"),  # spotify, musicbrainz, name_match
            Column("spotify_genres", String),  # JSON string
            Column("musicbrainz_genres", String),  # JSON string
            Column("cached_at", DateTime, default=datetime.datetime.now),
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
                existing = conn.execute(
                    select(self.tracks).where(self.tracks.c.spotify_id == spotify_id)
                ).fetchone()

                if existing and existing.tidal_id != tidal_id:
                    conn.execute(
                        update(self.tracks)
                        .where(self.tracks.c.spotify_id == spotify_id)
                        .values(
                            spotify_id=None,
                            match_confidence=None,
                            match_method=None,
                            matched_at=None,
                            updated_at=datetime.datetime.now(),
                        )
                    )

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

    def store_tidal_tracks_batch(self, tracks: list[dict]):
        """
        Batch insert/update Tidal tracks for better performance.
        tracks: list of dicts with keys: tidal_id, name, artists, album, isrc, duration, track_num
        """
        if not tracks:
            return
        values = []
        now = datetime.datetime.now()
        for t in tracks:
            values.append(
                {
                    "tidal_id": t["tidal_id"],
                    "tidal_name": t["name"],
                    "tidal_artists": json.dumps(t["artists"]),
                    "tidal_album": t.get("album"),
                    "tidal_isrc": t.get("isrc"),
                    "tidal_duration": t.get("duration"),
                    "tidal_track_num": t.get("track_num"),
                    "created_at": now,
                    "updated_at": now,
                }
            )
        with self.engine.connect() as conn:
            with conn.begin():
                conn.execute(insert(self.tracks), values)

    def store_spotify_tracks_batch(self, tracks: list[dict]):
        """
        Batch insert/update Spotify tracks for better performance.
        tracks: list of dicts with keys: spotify_id, name, artists, album, isrc, duration
        """
        if not tracks:
            return
        values = []
        now = datetime.datetime.now()
        for t in tracks:
            values.append(
                {
                    "spotify_id": t["spotify_id"],
                    "spotify_name": t["name"],
                    "spotify_artists": json.dumps(t["artists"]),
                    "spotify_album": t.get("album"),
                    "spotify_isrc": t.get("isrc"),
                    "spotify_duration": t.get("duration"),
                    "created_at": now,
                    "updated_at": now,
                }
            )
        with self.engine.connect() as conn:
            with conn.begin():
                conn.execute(insert(self.tracks), values)

    def store_matches_batch(self, matches: list[dict]):
        """
        Batch insert/update matches for better performance.
        matches: list of dicts with keys: tidal_id, spotify_id, confidence, method
        """
        if not matches:
            return
        now = datetime.datetime.now()
        with self.engine.connect() as conn:
            with conn.begin():
                for m in matches:
                    conn.execute(
                        update(self.tracks)
                        .where(self.tracks.c.tidal_id == m["tidal_id"])
                        .values(
                            spotify_id=m["spotify_id"],
                            match_confidence=m["confidence"],
                            match_method=m["method"],
                            matched_at=now,
                            updated_at=now,
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

    def cache_not_found(
        self,
        direction: str,
        track_id: str,
        track_name: str,
        track_artists: list[str],
        isrc: str | None = None,
    ):
        """Record a track that was not found on the target platform"""
        with self.engine.connect() as conn:
            with conn.begin():
                existing = conn.execute(
                    select(self.not_found).where(
                        and_(
                            self.not_found.c.direction == direction,
                            self.not_found.c.track_id == track_id,
                        )
                    )
                ).fetchone()

                if not existing:
                    conn.execute(
                        insert(self.not_found),
                        {
                            "direction": direction,
                            "track_id": track_id,
                            "track_name": track_name,
                            "track_artists": ", ".join(track_artists),
                            "isrc": isrc,
                            "searched_at": datetime.datetime.now(),
                        },
                    )

    def get_not_found(self, direction: str) -> list[dict]:
        """Get all tracks not found for a given direction"""
        with self.engine.connect() as conn:
            rows = conn.execute(
                select(self.not_found)
                .where(self.not_found.c.direction == direction)
                .order_by(self.not_found.c.searched_at.desc())
            ).fetchall()
            return [dict(row._mapping) for row in rows]

    def clear_not_found(self, direction: str | None = None):
        """Clear not found records. If direction is None, clear all."""
        with self.engine.connect() as conn:
            with conn.begin():
                if direction:
                    conn.execute(
                        delete(self.not_found).where(
                            self.not_found.c.direction == direction
                        )
                    )
                else:
                    conn.execute(delete(self.not_found))

    def log_not_found_to_file(self, direction: str):
        """
        Write not found tracks to a log file.
        Format: songs_not_found_[direction].txt
        """
        tracks = self.get_not_found(direction)

        if direction == "tidal_to_spotify":
            filename = "songs_not_found_tidal_to_spotify.txt"
            header = "TIDAL TRACKS NOT FOUND ON SPOTIFY"
        else:
            filename = "songs_not_found_spotify_to_tidal.txt"
            header = "SPOTIFY TRACKS NOT FOUND ON TIDAL"

        filepath = os.path.join(self.log_dir, filename)

        with open(filepath, "w", encoding="utf-8") as f:
            f.write("=" * 60 + "\n")
            f.write(header + "\n")
            f.write(
                f"Generated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            )
            f.write(f"Total: {len(tracks)} tracks\n")
            f.write("=" * 60 + "\n\n")

            if not tracks:
                f.write("No tracks missing.\n")
            else:
                for i, track in enumerate(tracks, 1):
                    f.write(f"{i}. {track['track_artists']} - {track['track_name']}\n")
                    if track.get("isrc"):
                        f.write(f"   ISRC: {track['isrc']}\n")
                    f.write(f"   ID: {track['track_id']}\n")
                    f.write(f"   Searched: {track['searched_at']}\n")
                    f.write("\n")

        return filepath

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

    def store_artist_genre(self, artist_id: str, artist_name: str, genre_category: str, spotify_genres: list, genre_source: str = "spotify", musicbrainz_genres: list = None):
        """Store artist genre mapping"""
        import json
        try:
            existing = self.get_artist_genre(artist_id)
            if existing:
                with self.engine.connect() as conn:
                    with conn.begin():
                        conn.execute(
                            self.artist_genres.update()
                            .where(self.artist_genres.c.artist_id == artist_id)
                            .values(
                                genre_category=genre_category,
                                genre_source=genre_source,
                                spotify_genres=json.dumps(spotify_genres),
                                musicbrainz_genres=json.dumps(musicbrainz_genres) if musicbrainz_genres else None,
                                cached_at=datetime.datetime.now(),
                            )
                        )
            else:
                with self.engine.connect() as conn:
                    with conn.begin():
                        conn.execute(
                            self.artist_genres.insert().values(
                                artist_id=artist_id,
                                artist_name=artist_name,
                                genre_category=genre_category,
                                genre_source=genre_source,
                                spotify_genres=json.dumps(spotify_genres),
                                musicbrainz_genres=json.dumps(musicbrainz_genres) if musicbrainz_genres else None,
                                cached_at=datetime.datetime.now(),
                            )
                        )
        except Exception:
            pass

    def store_artist_genre_musicbrainz(self, artist_id: str, artist_name: str, genre_category: str, musicbrainz_genres: list):
        """Store artist genre mapping from MusicBrainz"""
        import json
        try:
            existing = self.get_artist_genre(artist_id)
            if existing:
                with self.engine.connect() as conn:
                    with conn.begin():
                        conn.execute(
                            self.artist_genres.update()
                            .where(self.artist_genres.c.artist_id == artist_id)
                            .values(
                                genre_category=genre_category,
                                genre_source="musicbrainz",
                                musicbrainz_genres=json.dumps(musicbrainz_genres),
                                cached_at=datetime.datetime.now(),
                            )
                        )
            else:
                with self.engine.connect() as conn:
                    with conn.begin():
                        conn.execute(
                            self.artist_genres.insert().values(
                                artist_id=artist_id,
                                artist_name=artist_name,
                                genre_category=genre_category,
                                genre_source="musicbrainz",
                                musicbrainz_genres=json.dumps(musicbrainz_genres),
                                cached_at=datetime.datetime.now(),
                            )
                        )
        except Exception:
            pass

    def get_artist_genre(self, artist_id: str) -> str | None:
        """Get cached artist genre"""
        with self.engine.connect() as conn:
            result = conn.execute(
                select(self.artist_genres).where(self.artist_genres.c.artist_id == artist_id)
            ).fetchone()
            if result:
                return result.genre_category
            return None


unified_cache = UnifiedTrackCache()
