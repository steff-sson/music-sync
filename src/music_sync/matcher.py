#!/usr/bin/env python3

import unicodedata
from difflib import SequenceMatcher
from typing import Sequence, Set


def normalize(s: str) -> str:
    return unicodedata.normalize("NFD", s).encode("ascii", "ignore").decode("ascii")


def simple(s: str) -> str:
    return s.split("-")[0].strip().split("(")[0].strip().split("[")[0].strip()


class BidirectionalMatcher:
    """
    Handles matching logic for Tidal <-> Spotify track matching.
    Supports ISRC-based matching and fuzzy matching via name/artist/duration.
    """

    MATCH_METHOD_ISRC = "isrc"
    MATCH_METHOD_NAME_ARTIST_DURATION = "name_artist_duration"

    def isrc_match(self, source_isrc: str | None, target_isrc: str | None) -> bool:
        """Exact ISRC match"""
        if not source_isrc or not target_isrc:
            return False
        return source_isrc == target_isrc

    def duration_match(
        self,
        source_duration: int | None,
        target_duration: int | None,
        tolerance: int = 2,
    ) -> bool:
        """Duration match within tolerance (seconds)"""
        if not source_duration or not target_duration:
            return False
        return abs(source_duration - target_duration) < tolerance

    def name_match(
        self, source_name: str, target_name: str, source_version: str | None = None
    ) -> bool:
        """Check if simplified names match, handling exclusions"""

        def exclusion_rule(
            pattern: str, name1: str, name2: str, version1: str | None = None
        ) -> bool:
            has_pattern_1 = pattern in name1.lower() or (
                version1 is not None and pattern in version1.lower()
            )
            has_pattern_2 = pattern in name2.lower()
            return has_pattern_1 != has_pattern_2

        if exclusion_rule("instrumental", source_name, target_name, source_version):
            return False
        if exclusion_rule("acapella", source_name, target_name, source_version):
            return False
        if exclusion_rule("remix", source_name, target_name, source_version):
            return False

        simple_source = simple(source_name.lower()).split("feat.")[0].strip()
        simple_target = simple(target_name.lower())

        return simple_source in simple_target or normalize(simple_source) in normalize(
            simple_target
        )

    def artist_match(
        self, source_artists: Sequence[str], target_artists: Sequence[str]
    ) -> bool:
        """Check if any artist matches"""

        def split_artist_name(artist: str) -> Sequence[str]:
            if "&" in artist:
                return artist.split("&")
            elif "," in artist:
                return artist.split(",")
            else:
                return [artist]

        def get_normalized_artists(artists: Sequence[str]) -> Set[str]:
            result = []
            for artist in artists:
                result.extend(split_artist_name(artist))
            return set(simple(x.strip().lower()) for x in result)

        source_set = get_normalized_artists(source_artists)
        target_set = get_normalized_artists(target_artists)

        if source_set.intersection(target_set):
            return True

        source_norm = set(normalize(a) for a in source_set)
        target_norm = set(normalize(a) for a in target_set)
        return bool(source_norm.intersection(target_norm))

    def match_tracks(
        self,
        source_name: str,
        source_artists: Sequence[str],
        source_duration: int | None,
        source_isrc: str | None,
        source_version: str | None,
        target_name: str,
        target_artists: Sequence[str],
        target_duration: int | None,
        target_isrc: str | None,
    ) -> tuple[bool, float, str]:
        """
        Try to match two tracks.
        Returns: (matched, confidence, method)
        """

        if self.isrc_match(source_isrc, target_isrc):
            return (True, 1.0, self.MATCH_METHOD_ISRC)

        duration_ok = self.duration_match(source_duration, target_duration)
        name_ok = self.name_match(source_name, target_name, source_version)
        artist_ok = self.artist_match(source_artists, target_artists)

        if not (duration_ok and name_ok and artist_ok):
            return (False, 0.0, "")

        confidence = 0.0
        if name_ok:
            confidence += 0.4
        if artist_ok:
            confidence += 0.3
        if duration_ok:
            confidence += 0.3

        return (True, min(confidence, 0.9), self.MATCH_METHOD_NAME_ARTIST_DURATION)

    def match_tidal_to_spotify(
        self,
        tidal_track,
        spotify_track: dict,
    ) -> tuple[bool, float, str]:
        """Match a Tidal track to a Spotify track dict"""
        tidal_artists = [a.name for a in tidal_track.artists]
        spotify_artists = spotify_track.get("artists", [])
        if isinstance(spotify_artists[0], dict) if spotify_artists else False:
            spotify_artists = [a["name"] for a in spotify_artists]

        return self.match_tracks(
            source_name=tidal_track.name,
            source_artists=tidal_artists,
            source_duration=tidal_track.duration,
            source_isrc=tidal_track.isrc,
            source_version=getattr(tidal_track, "version", None),
            target_name=spotify_track.get("name", ""),
            target_artists=spotify_artists,
            target_duration=spotify_track.get("duration_ms", 0) // 1000
            if spotify_track.get("duration_ms")
            else None,
            target_isrc=spotify_track.get("external_ids", {}).get("isrc")
            if spotify_track.get("external_ids")
            else None,
        )

    def match_spotify_to_tidal(
        self,
        spotify_track: dict,
        tidal_track,
    ) -> tuple[bool, float, str]:
        """Match a Spotify track dict to a Tidal track"""
        spotify_artists = spotify_track.get("artists", [])
        if isinstance(spotify_artists[0], dict) if spotify_artists else False:
            spotify_artists = [a["name"] for a in spotify_artists]
        tidal_artists = [a.name for a in tidal_track.artists]

        return self.match_tracks(
            source_name=spotify_track.get("name", ""),
            source_artists=spotify_artists,
            source_duration=spotify_track.get("duration_ms", 0) // 1000
            if spotify_track.get("duration_ms")
            else None,
            source_isrc=spotify_track.get("external_ids", {}).get("isrc")
            if spotify_track.get("external_ids")
            else None,
            source_version=None,
            target_name=tidal_track.name,
            target_artists=tidal_artists,
            target_duration=tidal_track.duration,
            target_isrc=tidal_track.isrc,
        )


matcher = BidirectionalMatcher()
