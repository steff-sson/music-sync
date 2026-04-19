#!/usr/bin/env python3

import asyncio
from dataclasses import dataclass, field
from typing import Sequence
import tidalapi
import spotipy

from .cache_db import UnifiedTrackCache, unified_cache
from .matcher import matcher, BidirectionalMatcher


@dataclass
class SyncResult:
    """Result of a sync operation"""

    direction: str
    total_source_tracks: int = 0
    matched_tracks: int = 0
    new_tracks: int = 0
    not_found_tracks: list = field(default_factory=list)
    errors: list = field(default_factory=list)


class SyncEngine:
    """
    Cache-based bidirectional sync engine.
    Workflow:
    1. Load tracks from source → Store in cache
    2. Load tracks from target → Store in cache
    3. Match tracks (ISRC → name/artist/duration)
    4. Identify unmatched source tracks
    5. [If not dry-run] Send to target platform
    """

    def __init__(
        self,
        cache: UnifiedTrackCache | None = None,
        matcher: BidirectionalMatcher | None = None,
    ):
        self.cache = cache or unified_cache
        self.matcher = matcher or BidirectionalMatcher()

    async def load_tidal_tracks(
        self,
        tidal_session: tidalapi.Session,
        playlists: Sequence[tidalapi.Playlist] | None = None,
        load_favorites: bool = True,
    ) -> list[tidalapi.Track]:
        """Load tracks from Tidal and store in cache"""
        from .tidalapi_patch import get_all_playlist_tracks, get_all_favorites

        all_tracks = []

        if playlists:
            for playlist in playlists:
                tracks = await get_all_playlist_tracks(playlist)
                for track in tracks:
                    self._store_tidal_track(track)
                all_tracks.extend(tracks)

        if load_favorites:
            favorites = await get_all_favorites(
                tidal_session.user.favorites, order="DATE"
            )
            for track in favorites:
                self._store_tidal_track(track)
            all_tracks.extend(favorites)

        return all_tracks

    async def load_spotify_tracks(
        self,
        spotify_session: spotipy.Spotify,
        load_favorites: bool = True,
    ) -> list[dict]:
        """Load tracks from Spotify and store in cache"""
        all_tracks = []

        if load_favorites:
            tracks = await self._fetch_spotify_favorites(spotify_session)
            for track in tracks:
                self._store_spotify_track(track)
            all_tracks.extend(tracks)

        return all_tracks

    async def _fetch_spotify_favorites(
        self, spotify_session: spotipy.Spotify
    ) -> list[dict]:
        """Fetch all Spotify favorite tracks"""
        results = []
        offset = 0
        while True:
            chunk = spotify_session.current_user_saved_tracks(offset=offset)
            items = [item["track"] for item in chunk["items"] if item["track"]]
            results.extend(items)
            if not chunk["next"]:
                break
            offset += chunk["limit"]
        return results

    def _store_tidal_track(self, track: tidalapi.Track):
        """Store Tidal track metadata in cache"""
        artists = [a.name for a in track.artists]
        self.cache.store_tidal_track(
            tidal_id=track.id,
            name=track.name,
            artists=artists,
            album=track.album.name if track.album else None,
            isrc=track.isrc,
            duration=track.duration,
            track_num=track.track_num,
        )

    def _store_spotify_track(self, track: dict):
        """Store Spotify track metadata in cache"""
        artists = [a["name"] for a in track.get("artists", [])]
        isrc = None
        if track.get("external_ids"):
            isrc = track["external_ids"].get("isrc")
        self.cache.store_spotify_track(
            spotify_id=track["id"],
            name=track["name"],
            artists=artists,
            album=track.get("album", {}).get("name") if track.get("album") else None,
            isrc=isrc,
            duration=track.get("duration_ms", 0) // 1000
            if track.get("duration_ms")
            else None,
        )

    def find_spotify_match_for_tidal(
        self, tidal_track: tidalapi.Track
    ) -> tuple[str | None, float, str]:
        """
        Find Spotify match for a Tidal track.
        Returns: (spotify_id, confidence, method)
        """
        tidal_artists = [a.name for a in tidal_track.artists]

        cached = self.cache.find_tidal_to_spotify_match(
            tidal_isrc=tidal_track.isrc,
            tidal_name=tidal_track.name,
            tidal_artists=tidal_artists,
            tidal_duration=tidal_track.duration,
        )
        if cached and cached.get("spotify_id"):
            return (cached["spotify_id"], cached.get("match_confidence", 1.0), "cache")

        return (None, 0.0, "")

    def find_tidal_match_for_spotify(
        self, spotify_track: dict
    ) -> tuple[str | None, float, str]:
        """
        Find Tidal match for a Spotify track.
        Returns: (tidal_id, confidence, method)
        """
        spotify_artists = [a["name"] for a in spotify_track.get("artists", [])]
        spotify_isrc = None
        if spotify_track.get("external_ids"):
            spotify_isrc = spotify_track["external_ids"].get("isrc")

        cached = self.cache.find_spotify_to_tidal_match(
            spotify_isrc=spotify_isrc,
            spotify_name=spotify_track["name"],
            spotify_artists=spotify_artists,
            spotify_duration=spotify_track.get("duration_ms", 0) // 1000
            if spotify_track.get("duration_ms")
            else None,
        )
        if cached and cached.get("tidal_id"):
            return (cached["tidal_id"], cached.get("match_confidence", 1.0), "cache")

        return (None, 0.0, "")

    async def sync_tidal_to_spotify(
        self,
        tidal_session: tidalapi.Session,
        spotify_session: spotipy.Spotify,
        config: dict,
        dry_run: bool = False,
        playlists: Sequence[tidalapi.Playlist] | None = None,
        sync_favorites: bool = True,
    ) -> SyncResult:
        """
        Sync tracks from Tidal to Spotify.

        1. Load Tidal tracks → Cache
        2. Match against cache/Spotify
        3. Find unmatched Tidal tracks
        4. [If not dry-run] Add to Spotify
        """
        result = SyncResult(direction="tidal_to_spotify")

        print("Loading Tidal tracks...")
        tidal_tracks = await self.load_tidal_tracks(
            tidal_session, playlists, sync_favorites
        )
        result.total_source_tracks = len(tidal_tracks)
        print(f"  Loaded {len(tidal_tracks)} Tidal tracks")

        print("Finding Spotify matches for Tidal tracks...")
        matched = 0
        not_found = []

        for tidal_track in tidal_tracks:
            spotify_id, confidence, method = self.find_spotify_match_for_tidal(
                tidal_track
            )

            if spotify_id:
                matched += 1
                self.cache.store_match(
                    tidal_id=tidal_track.id,
                    spotify_id=spotify_id,
                    confidence=confidence,
                    method=method,
                )
            else:
                not_found.append(tidal_track)

        result.matched_tracks = matched
        result.not_found_tracks = not_found
        result.new_tracks = len([t for t in not_found if self._is_new_track(t)])

        if not dry_run:
            print(f"Adding {len(not_found)} tracks to Spotify...")
            for tidal_track in not_found:
                try:
                    self._add_tidal_track_to_spotify(tidal_track, spotify_session)
                except Exception as e:
                    result.errors.append(f"{tidal_track.name}: {e}")
        else:
            print(f"[DRY RUN] Would add {len(not_found)} tracks to Spotify")

        if not_found:
            self._log_not_found("tidal_to_spotify", not_found)

        return result

    async def sync_spotify_to_tidal(
        self,
        tidal_session: tidalapi.Session,
        spotify_session: spotipy.Spotify,
        config: dict,
        dry_run: bool = False,
        sync_favorites: bool = True,
    ) -> SyncResult:
        """
        Sync tracks from Spotify to Tidal.
        """
        result = SyncResult(direction="spotify_to_tidal")

        print("Loading Spotify tracks...")
        spotify_tracks = await self.load_spotify_tracks(spotify_session, sync_favorites)
        result.total_source_tracks = len(spotify_tracks)
        print(f"  Loaded {len(spotify_tracks)} Spotify tracks")

        print("Finding Tidal matches for Spotify tracks...")
        matched = 0
        not_found = []

        for spotify_track in spotify_tracks:
            tidal_id, confidence, method = self.find_tidal_match_for_spotify(
                spotify_track
            )

            if tidal_id:
                matched += 1
                self.cache.store_match(
                    tidal_id=tidal_id,
                    spotify_id=spotify_track["id"],
                    confidence=confidence,
                    method=method,
                )
            else:
                not_found.append(spotify_track)

        result.matched_tracks = matched
        result.not_found_tracks = not_found

        if not dry_run:
            print(f"Adding {len(not_found)} tracks to Tidal...")
            for spotify_track in not_found:
                try:
                    self._add_spotify_track_to_tidal(spotify_track, tidal_session)
                except Exception as e:
                    result.errors.append(f"{spotify_track['name']}: {e}")
        else:
            print(f"[DRY RUN] Would add {len(not_found)} tracks to Tidal")

        if not_found:
            self._log_not_found("spotify_to_tidal", not_found)

        return result

    def _is_new_track(self, tidal_track) -> bool:
        """Check if track was newly added (not in existing Spotify)"""
        cached = self.cache.get_by_tidal_id(tidal_track.id)
        return not cached or not cached.get("spotify_id")

    def _add_tidal_track_to_spotify(
        self, tidal_track: tidalapi.Track, spotify_session: spotipy.Spotify
    ):
        """Add a Tidal track to Spotify (requires prior match)"""
        spotify_id, _, _ = self.find_spotify_match_for_tidal(tidal_track)
        if not spotify_id:
            raise ValueError(f"No Spotify match found for {tidal_track.name}")
        spotify_session.current_user_saved_tracks_add([spotify_id])

    def _add_spotify_track_to_tidal(
        self, spotify_track: dict, tidal_session: tidalapi.Session
    ):
        """Add a Spotify track to Tidal (requires prior match)"""
        tidal_id, _, _ = self.find_tidal_match_for_spotify(spotify_track)
        if not tidal_id:
            raise ValueError(f"No Tidal match found for {spotify_track['name']}")
        tidal_session.user.favorites.add_track(tidal_id)

    def _log_not_found(self, direction: str, tracks: list):
        """Log tracks not found to file"""
        for track in tracks:
            if hasattr(track, "name"):
                name = track.name
                artists = [a.name for a in track.artists]
                isrc = getattr(track, "isrc", None)
                track_id = track.id
            else:
                name = track.get("name", "Unknown")
                artists = [a["name"] for a in track.get("artists", [])]
                isrc = (
                    track.get("external_ids", {}).get("isrc")
                    if track.get("external_ids")
                    else None
                )
                track_id = track.get("id", "Unknown")

            self.cache.cache_not_found(
                direction=direction,
                track_id=track_id,
                track_name=name,
                track_artists=artists,
                isrc=isrc,
            )

        self.cache.log_not_found_to_file(direction)


sync_engine = SyncEngine()
