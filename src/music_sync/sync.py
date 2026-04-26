#!/usr/bin/env python3

import asyncio
import logging
from collections import defaultdict
from .cache import (
    failure_cache,
    track_match_cache,
    reverse_failure_cache,
    unified_cache,
)
import datetime
from difflib import SequenceMatcher
from functools import partial
from typing import Callable, List, Sequence, Set, Mapping
import math
import requests


def log(*args, **kwargs):
    msg = " ".join(str(a) for a in args)
    print(msg, **kwargs)  # Note: Using print() here, not log() to avoid recursion
    logging.info(msg)


import sys
import spotipy
import tidalapi
from .tidalapi_patch import (
    add_multiple_tracks_to_playlist,
    clear_tidal_playlist,
    get_all_favorites,
    get_all_playlists,
    get_all_playlist_tracks,
)
import time
from tqdm.asyncio import tqdm as atqdm
from tqdm import tqdm
import traceback
import unicodedata
import math

from .type import spotify as t_spotify


def normalize(s) -> str:
    return unicodedata.normalize("NFD", s).encode("ascii", "ignore").decode("ascii")


def simple(input_string: str) -> str:
    # only take the first part of a string before any hyphens or brackets to account for different versions
    return (
        input_string.split("-")[0].strip().split("(")[0].strip().split("[")[0].strip()
    )


def isrc_match(tidal_track: tidalapi.Track, spotify_track) -> bool:
    if "isrc" in spotify_track["external_ids"]:
        return tidal_track.isrc == spotify_track["external_ids"]["isrc"]
    return False


def duration_match(tidal_track: tidalapi.Track, spotify_track, tolerance=2) -> bool:
    # the duration of the two tracks must be the same to within 2 seconds
    return abs(tidal_track.duration - spotify_track["duration_ms"] / 1000) < tolerance


def name_match(tidal_track, spotify_track) -> bool:
    def exclusion_rule(
        pattern: str, tidal_track: tidalapi.Track, spotify_track: t_spotify.SpotifyTrack
    ):
        spotify_has_pattern = pattern in spotify_track["name"].lower()
        tidal_has_pattern = pattern in tidal_track.name.lower() or (
            not tidal_track.version is None and (pattern in tidal_track.version.lower())
        )
        return spotify_has_pattern != tidal_has_pattern

    # handle some edge cases
    if exclusion_rule("instrumental", tidal_track, spotify_track):
        return False
    if exclusion_rule("acapella", tidal_track, spotify_track):
        return False
    if exclusion_rule("remix", tidal_track, spotify_track):
        return False

    # the simplified version of the Spotify track name must be a substring of the Tidal track name
    # Try with both un-normalized and then normalized
    simple_spotify_track = (
        simple(spotify_track["name"].lower()).split("feat.")[0].strip()
    )
    return simple_spotify_track in tidal_track.name.lower() or normalize(
        simple_spotify_track
    ) in normalize(tidal_track.name.lower())


def artist_match(tidal: tidalapi.Track | tidalapi.Album, spotify) -> bool:
    def split_artist_name(artist: str) -> Sequence[str]:
        if "&" in artist:
            return artist.split("&")
        elif "," in artist:
            return artist.split(",")
        else:
            return [artist]

    def get_tidal_artists(
        tidal: tidalapi.Track | tidalapi.Album, do_normalize=False
    ) -> Set[str]:
        result: list[str] = []
        for artist in tidal.artists:
            if do_normalize:
                artist_name = normalize(artist.name)
            else:
                artist_name = artist.name
            result.extend(split_artist_name(artist_name))
        return set([simple(x.strip().lower()) for x in result])

    def get_spotify_artists(spotify, do_normalize=False) -> Set[str]:
        result: list[str] = []
        for artist in spotify["artists"]:
            if do_normalize:
                artist_name = normalize(artist["name"])
            else:
                artist_name = artist["name"]
            result.extend(split_artist_name(artist_name))
        return set([simple(x.strip().lower()) for x in result])

    # There must be at least one overlapping artist between the Tidal and Spotify track
    # Try with both un-normalized and then normalized
    if get_tidal_artists(tidal).intersection(get_spotify_artists(spotify)) != set():
        return True
    return (
        get_tidal_artists(tidal, True).intersection(get_spotify_artists(spotify, True))
        != set()
    )


def match(tidal_track, spotify_track) -> bool:
    if not spotify_track["id"]:
        return False
    return isrc_match(tidal_track, spotify_track) or (
        duration_match(tidal_track, spotify_track)
        and name_match(tidal_track, spotify_track)
        and artist_match(tidal_track, spotify_track)
    )


def test_album_similarity(spotify_album, tidal_album, threshold=0.6):
    return SequenceMatcher(
        None, simple(spotify_album["name"]), simple(tidal_album.name)
    ).ratio() >= threshold and artist_match(tidal_album, spotify_album)


async def tidal_search(
    spotify_track, rate_limiter, tidal_session: tidalapi.Session
) -> tidalapi.Track | None:
    def _search_for_track_in_album():
        # search for album name and first album artist
        if (
            "album" in spotify_track
            and "artists" in spotify_track["album"]
            and len(spotify_track["album"]["artists"])
        ):
            query = (
                simple(spotify_track["album"]["name"])
                + " "
                + simple(spotify_track["album"]["artists"][0]["name"])
            )
            album_result = tidal_session.search(query, models=[tidalapi.album.Album])
            for album in album_result["albums"]:
                if album.num_tracks >= spotify_track[
                    "track_number"
                ] and test_album_similarity(spotify_track["album"], album):
                    album_tracks = album.tracks()
                    if len(album_tracks) < spotify_track["track_number"]:
                        assert (
                            not len(album_tracks) == album.num_tracks
                        )  # incorrect metadata :(
                        continue
                    track = album_tracks[spotify_track["track_number"] - 1]
                    if match(track, spotify_track):
                        failure_cache.remove_match_failure(spotify_track["id"])
                        return track

    def _search_for_standalone_track():
        # if album search fails then search for track name and first artist
        query = (
            simple(spotify_track["name"])
            + " "
            + simple(spotify_track["artists"][0]["name"])
        )
        for track in tidal_session.search(query, models=[tidalapi.media.Track])[
            "tracks"
        ]:
            if match(track, spotify_track):
                failure_cache.remove_match_failure(spotify_track["id"])
                return track

    await rate_limiter.acquire()
    album_search = await asyncio.to_thread(_search_for_track_in_album)
    if album_search:
        return album_search
    await rate_limiter.acquire()
    track_search = await asyncio.to_thread(_search_for_standalone_track)
    if track_search:
        return track_search

    # if none of the search modes succeeded then store the track id to the failure cache
    failure_cache.cache_match_failure(spotify_track["id"])


async def repeat_on_request_error(function, *args, remaining=5, **kwargs):
    try:
        return await function(*args, **kwargs)
    except (
        tidalapi.exceptions.TooManyRequests,
        requests.exceptions.RequestException,
        spotipy.exceptions.SpotifyException,
    ) as e:
        if remaining:
            log(f"{str(e)} occurred, retrying {remaining} times")
        else:
            log(f"{str(e)} could not be recovered")

        retry_after = None
        if isinstance(e, spotipy.exceptions.SpotifyException):
            if e.http_status == 429:
                log(f"Spotify rate limit hit (429)")
                if "rate_limit_state" in globals():
                    rate_limit_state["consecutive_429s"] += 1
                    backoff_seconds = min(
                        30 * (2 ** rate_limit_state["consecutive_429s"]), 300
                    )
                    now = time.time()
                    rate_limit_state["cooldown_until"] = now + backoff_seconds
                    log(f"Entering cooldown for {backoff_seconds}s")
        elif (
            isinstance(e, requests.exceptions.RequestException)
            and e.response is not None
        ):
            log(f"Response message: {e.response.text}")
            log(f"Response headers: {e.response.headers}")
            retry_after = e.response.headers.get("Retry-After")

        if not remaining:
            log("Aborting sync")
            log(f"The following arguments were provided:\n\n {str(args)}")
            log(traceback.format_exc())
            sys.exit(1)

        if retry_after:
            sleep_time = max(int(retry_after), 1)
        else:
            sleep_schedule = {
                5: 1,
                4: 10,
                3: 60,
                2: 5 * 60,
                1: 10 * 60,
            }
            sleep_time = sleep_schedule.get(remaining, 1)

        time.sleep(sleep_time)
        return await repeat_on_request_error(
            function, *args, remaining=remaining - 1, **kwargs
        )


async def _fetch_all_from_spotify_in_chunks(fetch_function: Callable) -> List[dict]:
    output = []
    results = fetch_function(0)
    output.extend(
        [item["track"] for item in results["items"] if item["track"] is not None]
    )

    # Get all the remaining tracks in parallel
    if results["next"]:
        offsets = [
            results["limit"] * n
            for n in range(1, math.ceil(results["total"] / results["limit"]))
        ]
        extra_results = await atqdm.gather(
            *[asyncio.to_thread(fetch_function, offset) for offset in offsets],
            desc="Fetching additional data chunks",
        )
        for extra_result in extra_results:
            output.extend(
                [
                    item["track"]
                    for item in extra_result["items"]
                    if item["track"] is not None
                ]
            )

    return output


async def get_tracks_from_spotify_playlist(
    spotify_session: spotipy.Spotify, spotify_playlist
):
    def _get_tracks_from_spotify_playlist(offset: int, playlist_id: str):
        fields = "next,total,limit,items(track(name,album(name,artists),artists,track_number,duration_ms,id,external_ids(isrc))),type"
        return spotify_session.playlist_tracks(
            playlist_id=playlist_id, fields=fields, offset=offset
        )

    log(f"Loading tracks from Spotify playlist '{spotify_playlist['name']}'")
    items = await repeat_on_request_error(
        _fetch_all_from_spotify_in_chunks,
        lambda offset: _get_tracks_from_spotify_playlist(
            offset=offset, playlist_id=spotify_playlist["id"]
        ),
    )
    track_filter = lambda item: (
        item.get("type", "track") == "track"
    )  # type may be 'episode' also
    sanity_filter = lambda item: (
        "album" in item
        and "name" in item["album"]
        and "artists" in item["album"]
        and len(item["album"]["artists"]) > 0
        and item["album"]["artists"][0]["name"] is not None
    )
    return list(filter(sanity_filter, filter(track_filter, items)))


def populate_track_match_cache(
    spotify_tracks_: Sequence[t_spotify.SpotifyTrack],
    tidal_tracks_: Sequence[tidalapi.Track],
):
    """Populate the track match cache with all the existing tracks in Tidal playlist corresponding to Spotify playlist"""

    def _populate_one_track_from_spotify(spotify_track: t_spotify.SpotifyTrack):
        for idx, tidal_track in list(enumerate(tidal_tracks)):
            if tidal_track.available and match(tidal_track, spotify_track):
                track_match_cache.insert((spotify_track["id"], tidal_track.id))
                tidal_tracks.pop(idx)
                return

    def _populate_one_track_from_tidal(tidal_track: tidalapi.Track):
        for idx, spotify_track in list(enumerate(spotify_tracks)):
            if tidal_track.available and match(tidal_track, spotify_track):
                track_match_cache.insert((spotify_track["id"], tidal_track.id))
                spotify_tracks.pop(idx)
                return

    # make a copy of the tracks to avoid modifying original arrays
    spotify_tracks = [t for t in spotify_tracks_]
    tidal_tracks = [t for t in tidal_tracks_]

    # first populate from the tidal tracks
    for track in tidal_tracks:
        _populate_one_track_from_tidal(track)
    # then populate from the subset of Spotify tracks that didn't match (to account for many-to-one style mappings)
    for track in spotify_tracks:
        _populate_one_track_from_spotify(track)


def get_new_spotify_tracks(
    spotify_tracks: Sequence[t_spotify.SpotifyTrack],
) -> List[t_spotify.SpotifyTrack]:
    """Extracts only the tracks that have not already been seen in our Tidal caches"""
    results = []
    for spotify_track in spotify_tracks:
        if not spotify_track["id"]:
            continue
        if not track_match_cache.get(
            spotify_track["id"]
        ) and not failure_cache.has_match_failure(spotify_track["id"]):
            results.append(spotify_track)
    return results


def get_tracks_for_new_tidal_playlist(
    spotify_tracks: Sequence[t_spotify.SpotifyTrack],
) -> Sequence[int]:
    """gets list of corresponding tidal track ids for each spotify track, ignoring duplicates"""
    output = []
    seen_tracks = set()

    for spotify_track in spotify_tracks:
        if not spotify_track["id"]:
            continue
        tidal_id = track_match_cache.get(spotify_track["id"])
        if tidal_id:
            if tidal_id in seen_tracks:
                track_name = spotify_track["name"]
                artist_names = ", ".join(
                    [artist["name"] for artist in spotify_track["artists"]]
                )
                log(
                    f'Duplicate found: Track "{track_name}" by {artist_names} will be ignored'
                )
            else:
                output.append(tidal_id)
                seen_tracks.add(tidal_id)
    return output


async def search_new_tracks_on_tidal(
    tidal_session: tidalapi.Session,
    spotify_tracks: Sequence[t_spotify.SpotifyTrack],
    playlist_name: str,
    config: dict,
):
    """Generic function for searching for each item in a list of Spotify tracks which have not already been seen and adding them to the cache"""

    async def _run_rate_limiter(semaphore):
        """Leaky bucket algorithm for rate limiting. Periodically releases items from semaphore at rate_limit"""
        _sleep_time = (
            config.get("max_concurrency", 10) / config.get("rate_limit", 10) / 4
        )  # aim to sleep approx time to drain 1/4 of 'bucket'
        t0 = datetime.datetime.now()
        while True:
            await asyncio.sleep(_sleep_time)
            t = datetime.datetime.now()
            dt = (t - t0).total_seconds()
            new_items = round(config.get("rate_limit", 10) * dt)
            t0 = t
            [
                semaphore.release() for i in range(new_items)
            ]  # leak new_items from the 'bucket'

    # Extract the new tracks that do not already exist in the old tidal tracklist
    tracks_to_search = get_new_spotify_tracks(spotify_tracks)
    if not tracks_to_search:
        return

    # Search for each of the tracks on Tidal concurrently
    task_description = (
        "Searching Tidal for {}/{} tracks in Spotify playlist '{}'".format(
            len(tracks_to_search), len(spotify_tracks), playlist_name
        )
    )
    semaphore = asyncio.Semaphore(config.get("max_concurrency", 10))
    rate_limiter_task = asyncio.create_task(_run_rate_limiter(semaphore))
    search_results = await atqdm.gather(
        *[
            repeat_on_request_error(tidal_search, t, semaphore, tidal_session)
            for t in tracks_to_search
        ],
        desc=task_description,
    )
    rate_limiter_task.cancel()

    # Add the search results to the cache
    song404 = []
    for idx, spotify_track in enumerate(tracks_to_search):
        if search_results[idx]:
            track_match_cache.insert((spotify_track["id"], search_results[idx].id))
        else:
            song404.append(
                f"{spotify_track['id']}: {','.join([a['name'] for a in spotify_track['artists']])} - {spotify_track['name']}"
            )
            color = ("\033[91m", "\033[0m")
            log(color[0] + "Could not find the track " + song404[-1] + color[1])
    file_name = "songs not found.txt"
    header = f"==========================\nPlaylist: {playlist_name}\n==========================\n"
    with open(file_name, "a", encoding="utf-8") as file:
        file.write(header)
        for song in song404:
            file.write(f"{song}\n")


async def sync_playlist(
    spotify_session: spotipy.Spotify,
    tidal_session: tidalapi.Session,
    spotify_playlist,
    tidal_playlist: tidalapi.Playlist | None,
    config: dict,
):
    """sync given playlist to tidal"""
    # Get the tracks from both Spotify and Tidal, creating a new Tidal playlist if necessary
    spotify_tracks = await get_tracks_from_spotify_playlist(
        spotify_session, spotify_playlist
    )
    if len(spotify_tracks) == 0:
        return  # nothing to do
    if tidal_playlist:
        old_tidal_tracks = await get_all_playlist_tracks(tidal_playlist)
    else:
        log(
            f"No playlist found on Tidal corresponding to Spotify playlist: '{spotify_playlist['name']}', creating new playlist"
        )
        tidal_playlist = tidal_session.user.create_playlist(
            spotify_playlist["name"], spotify_playlist["description"]
        )
        old_tidal_tracks = []

    # Extract the new tracks from the playlist that we haven't already seen before
    populate_track_match_cache(spotify_tracks, old_tidal_tracks)
    await search_new_tracks_on_tidal(
        tidal_session, spotify_tracks, spotify_playlist["name"], config
    )
    new_tidal_track_ids = get_tracks_for_new_tidal_playlist(spotify_tracks)

    # Update the Tidal playlist if there are changes
    old_tidal_track_ids = [t.id for t in old_tidal_tracks]
    if new_tidal_track_ids == old_tidal_track_ids:
        log("No changes to write to Tidal playlist")
    elif new_tidal_track_ids[: len(old_tidal_track_ids)] == old_tidal_track_ids:
        # Append new tracks to the existing playlist if possible
        add_multiple_tracks_to_playlist(
            tidal_playlist, new_tidal_track_ids[len(old_tidal_track_ids) :]
        )
    else:
        # Erase old playlist and add new tracks from scratch if any reordering occured
        clear_tidal_playlist(tidal_playlist)
        add_multiple_tracks_to_playlist(tidal_playlist, new_tidal_track_ids)


async def sync_favorites(
    spotify_session: spotipy.Spotify, tidal_session: tidalapi.Session, config: dict
):
    """sync user favorites to tidal"""

    async def get_tracks_from_spotify_favorites() -> List[dict]:
        _get_favorite_tracks = lambda offset: spotify_session.current_user_saved_tracks(
            offset=offset
        )
        tracks = await repeat_on_request_error(
            _fetch_all_from_spotify_in_chunks, _get_favorite_tracks
        )
        tracks.reverse()
        return tracks

    def get_new_tidal_favorites() -> List[int]:
        existing_favorite_ids = set([track.id for track in old_tidal_tracks])
        new_ids = []
        for spotify_track in spotify_tracks:
            match_id = track_match_cache.get(spotify_track["id"])
            if match_id and not match_id in existing_favorite_ids:
                new_ids.append(match_id)
        return new_ids

    log("Loading favorite tracks from Spotify")
    spotify_tracks = await get_tracks_from_spotify_favorites()
    log("Loading existing favorite tracks from Tidal")
    old_tidal_tracks = await get_all_favorites(
        tidal_session.user.favorites, order="DATE"
    )
    populate_track_match_cache(spotify_tracks, old_tidal_tracks)
    await search_new_tracks_on_tidal(tidal_session, spotify_tracks, "Favorites", config)
    new_tidal_favorite_ids = get_new_tidal_favorites()
    if new_tidal_favorite_ids:
        for tidal_id in tqdm(
            new_tidal_favorite_ids, desc="Adding new tracks to Tidal favorites"
        ):
            tidal_session.user.favorites.add_track(tidal_id)
    else:
        log("No new tracks to add to Tidal favorites")


def sync_playlists_wrapper(
    spotify_session: spotipy.Spotify,
    tidal_session: tidalapi.Session,
    playlists,
    config: dict,
):
    for spotify_playlist, tidal_playlist in playlists:
        # sync the spotify playlist to tidal
        asyncio.run(
            sync_playlist(
                spotify_session, tidal_session, spotify_playlist, tidal_playlist, config
            )
        )


def sync_favorites_wrapper(
    spotify_session: spotipy.Spotify, tidal_session: tidalapi.Session, config
):
    asyncio.run(
        main=sync_favorites(
            spotify_session=spotify_session, tidal_session=tidal_session, config=config
        )
    )


def get_tidal_playlists_wrapper(
    tidal_session: tidalapi.Session,
) -> Mapping[str, tidalapi.Playlist]:
    tidal_playlists = asyncio.run(get_all_playlists(tidal_session.user))
    return {playlist.name: playlist for playlist in tidal_playlists}


def pick_tidal_playlist_for_spotify_playlist(
    spotify_playlist, tidal_playlists: Mapping[str, tidalapi.Playlist]
):
    if spotify_playlist["name"] in tidal_playlists:
        # if there's an existing tidal playlist with the name of the current playlist then use that
        tidal_playlist = tidal_playlists[spotify_playlist["name"]]
        return (spotify_playlist, tidal_playlist)
    else:
        return (spotify_playlist, None)


def get_user_playlist_mappings(
    spotify_session: spotipy.Spotify, tidal_session: tidalapi.Session, config
):
    results = []
    spotify_playlists = asyncio.run(get_playlists_from_spotify(spotify_session, config))
    tidal_playlists = get_tidal_playlists_wrapper(tidal_session)
    for spotify_playlist in spotify_playlists:
        results.append(
            pick_tidal_playlist_for_spotify_playlist(spotify_playlist, tidal_playlists)
        )
    return results


async def get_playlists_from_spotify(spotify_session: spotipy.Spotify, config):
    # get all the playlists from the Spotify account
    playlists = []
    log("Loading Spotify playlists")
    first_results = spotify_session.current_user_playlists()
    exclude_list = set([x.split(":")[-1] for x in config.get("excluded_playlists", [])])
    playlists.extend([p for p in first_results["items"]])
    user_id = spotify_session.current_user()["id"]

    # get all the remaining playlists in parallel
    if first_results["next"]:
        offsets = [
            first_results["limit"] * n
            for n in range(
                1, math.ceil(first_results["total"] / first_results["limit"])
            )
        ]
        extra_results = await atqdm.gather(
            *[
                asyncio.to_thread(spotify_session.current_user_playlists, offset=offset)
                for offset in offsets
            ]
        )
        for extra_result in extra_results:
            playlists.extend([p for p in extra_result["items"]])

    # filter out playlists that don't belong to us or are on the exclude list
    my_playlist_filter = lambda p: p and p["owner"]["id"] == user_id
    exclude_filter = lambda p: not p["id"] in exclude_list
    return list(filter(exclude_filter, filter(my_playlist_filter, playlists)))


def get_playlists_from_config(
    spotify_session: spotipy.Spotify, tidal_session: tidalapi.Session, config
):
    # get the list of playlist sync mappings from the configuration file
    def get_playlist_ids(config):
        return [
            (item["spotify_id"], item["tidal_id"]) for item in config["sync_playlists"]
        ]

    output = []
    for spotify_id, tidal_id in get_playlist_ids(config=config):
        try:
            spotify_playlist = spotify_session.playlist(playlist_id=spotify_id)
        except spotipy.SpotifyException as e:
            log(f"Error getting Spotify playlist {spotify_id}")
            raise e
        try:
            tidal_playlist = tidal_session.playlist(playlist_id=tidal_id)
        except Exception as e:
            log(f"Error getting Tidal playlist {tidal_id}")
            raise e
        output.append((spotify_playlist, tidal_playlist))
    return output


# === TIDAL TO SPOTIFY SYNC FUNCTIONS ===


def reverse_isrc_match(spotify_track, tidal_track: tidalapi.Track) -> bool:
    if not tidal_track.isrc:
        return False
    if "isrc" in spotify_track["external_ids"]:
        return spotify_track["external_ids"]["isrc"] == tidal_track.isrc
    return False


def reverse_duration_match(
    spotify_track, tidal_track: tidalapi.Track, tolerance=2
) -> bool:
    return abs(spotify_track["duration_ms"] / 1000 - tidal_track.duration) < tolerance


def reverse_name_match(spotify_track, tidal_track: tidalapi.Track) -> bool:
    def exclusion_rule(pattern: str, spotify_track, tidal_track: tidalapi.Track):
        spotify_has_pattern = pattern in spotify_track["name"].lower()
        tidal_has_pattern = pattern in tidal_track.name.lower() or (
            not tidal_track.version is None and (pattern in tidal_track.version.lower())
        )
        return spotify_has_pattern != tidal_has_pattern

    if exclusion_rule("instrumental", spotify_track, tidal_track):
        return False
    if exclusion_rule("acapella", spotify_track, tidal_track):
        return False
    if exclusion_rule("remix", spotify_track, tidal_track):
        return False

    simple_tidal = simple(tidal_track.name.lower()).split("feat.")[0].strip()
    return simple_tidal in spotify_track["name"].lower() or normalize(
        simple_tidal
    ) in normalize(spotify_track["name"].lower())


def reverse_artist_match(spotify, tidal_track: tidalapi.Track) -> bool:
    def split_artist_name(artist: str) -> Sequence[str]:
        if "&" in artist:
            return artist.split("&")
        elif "," in artist:
            return artist.split(",")
        else:
            return [artist]

    def get_tidal_artists(tidal: tidalapi.Track, do_normalize=False) -> Set[str]:
        result: list[str] = []
        for artist in tidal.artists:
            if do_normalize:
                artist_name = normalize(artist.name)
            else:
                artist_name = artist.name
            result.extend(split_artist_name(artist_name))
        return set([simple(x.strip().lower()) for x in result])

    def get_spotify_artists(spotify, do_normalize=False) -> Set[str]:
        result: list[str] = []
        for artist in spotify["artists"]:
            if do_normalize:
                artist_name = normalize(artist["name"])
            else:
                artist_name = artist["name"]
            result.extend(split_artist_name(artist_name))
        return set([simple(x.strip().lower()) for x in result])

    if (
        get_spotify_artists(spotify).intersection(get_tidal_artists(tidal_track))
        != set()
    ):
        return True
    return (
        get_spotify_artists(spotify, True).intersection(
            get_tidal_artists(tidal_track, True)
        )
        != set()
    )


def reverse_match(spotify_track, tidal_track: tidalapi.Track) -> bool:
    if not spotify_track["id"]:
        return False
    return reverse_isrc_match(spotify_track, tidal_track) or (
        reverse_duration_match(spotify_track, tidal_track)
        and reverse_name_match(spotify_track, tidal_track)
        and reverse_artist_match(spotify_track, tidal_track)
    )


async def spotify_search(
    tidal_track: tidalapi.Track, rate_limiter, spotify_session: spotipy.Spotify
) -> str | None:
    def _search_by_isrc():
        if tidal_track.isrc:
            result = spotify_session.search(
                f"isrc:{tidal_track.isrc}", type="track", limit=5
            )
            for track in result["tracks"]["items"]:
                if reverse_match(track, tidal_track):
                    return track["id"]

    def _search_by_album():
        if tidal_track.album and tidal_track.track_num:
            album_name = (
                simple(tidal_track.album.name) if tidal_track.album.name else ""
            )
            if tidal_track.artists:
                query = f"{album_name} {simple(tidal_track.artists[0].name)}"
                album_result = spotify_session.search(query, type="album", limit=10)
                for album in album_result["albums"]["items"]:
                    tracks_page = getattr(album, "tracks", None) or album.get(
                        "tracks", {}
                    )
                    if callable(tracks_page):
                        tracks_items = tracks_page()
                    elif isinstance(tracks_page, dict):
                        tracks_items = tracks_page.get("items", [])
                    else:
                        tracks_items = getattr(tracks_page, "items", [])
                    track_num = tidal_track.track_num
                    if track_num <= len(tracks_items):
                        track = tracks_items[track_num - 1]
                        if reverse_match(track, tidal_track):
                            return track["id"]

    def _search_by_track_artist():
        query = f"{simple(tidal_track.name)} {simple(tidal_track.artists[0].name)}"
        result = spotify_session.search(query, type="track", limit=10)
        for track in result["tracks"]["items"]:
            if reverse_match(track, tidal_track):
                return track["id"]

    await rate_limiter.acquire()
    isrc_result = await asyncio.to_thread(_search_by_isrc)
    if isrc_result:
        reverse_failure_cache.remove_match_failure(tidal_track.id)
        return isrc_result

    await rate_limiter.acquire()
    album_result = await asyncio.to_thread(_search_by_album)
    if album_result:
        reverse_failure_cache.remove_match_failure(tidal_track.id)
        return album_result

    await rate_limiter.acquire()
    track_result = await asyncio.to_thread(_search_by_track_artist)
    if track_result:
        reverse_failure_cache.remove_match_failure(tidal_track.id)
    else:
        reverse_failure_cache.cache_match_failure(tidal_track.id)
    return track_result


async def search_new_tracks_on_spotify(
    spotify_session: spotipy.Spotify,
    tidal_tracks: Sequence[tidalapi.Track],
    description: str,
    config: dict,
    dry_run: bool = False,
):
    rate_limit_state = {"cooldown_until": 0, "consecutive_429s": 0}

    async def _run_rate_limiter(semaphore):
        rate = config.get("rate_limit", 3)
        base_rate = rate
        _sleep_time = rate / 4
        t0 = datetime.datetime.now()
        while True:
            now = time.time()
            if rate_limit_state["cooldown_until"] > now:
                sleep_duration = rate_limit_state["cooldown_until"] - now
                await asyncio.sleep(min(sleep_duration, 1.0))
                continue
            await asyncio.sleep(_sleep_time)
            t = datetime.datetime.now()
            dt = (t - t0).total_seconds()
            new_items = round(rate * dt)
            t0 = t
            for i in range(new_items):
                try:
                    semaphore.release()
                except RuntimeError:
                    pass

    def _on_429():
        now = time.time()
        rate_limit_state["consecutive_429s"] += 1
        backoff_seconds = min(30 * (2 ** rate_limit_state["consecutive_429s"]), 300)
        rate_limit_state["cooldown_until"] = now + backoff_seconds
        log(
            f"Rate limit hit! Entering cooldown for {backoff_seconds}s (consecutive: {rate_limit_state['consecutive_429s']})"
        )

    async def _check_and_handle_429(semaphore):
        pass

    semaphore = asyncio.Semaphore(config.get("max_concurrency", 3))
    rate_limiter_task = asyncio.create_task(_run_rate_limiter(semaphore))

    tracks_to_search = [
        t for t in tidal_tracks if not reverse_failure_cache.has_match_failure(t.id)
    ]
    if len(tracks_to_search) < len(tidal_tracks):
        skipped = len(tidal_tracks) - len(tracks_to_search)
        log(f"Skipping {skipped} previously failed tracks")

    search_results = await atqdm.gather(
        *[
            repeat_on_request_error(spotify_search, t, semaphore, spotify_session)
            for t in tracks_to_search
        ],
        desc=f"Searching Spotify for {description}",
    )
    rate_limiter_task.cancel()

    results_map = {}
    for idx, track in enumerate(tracks_to_search):
        results_map[track.id] = search_results[idx]
    for track in tidal_tracks:
        if track.id not in results_map:
            results_map[track.id] = None
    return [results_map[t.id] for t in tidal_tracks]


async def get_tidal_playlist_tracks(
    tidal_playlist: tidalapi.Playlist,
) -> List[tidalapi.Track]:
    return await get_all_playlist_tracks(tidal_playlist)


async def get_tidal_favorites_tracks(
    tidal_session: tidalapi.Session,
) -> List[tidalapi.Track]:
    return await get_all_favorites(tidal_session.user.favorites, order="DATE")


async def get_all_spotify_playlists_tracks(
    spotify_session: spotipy.Spotify,
) -> Mapping[str, List[dict]]:
    result = {}
    # Load ALL playlists (pagination fix)
    first = spotify_session.current_user_playlists()
    playlists = list(first["items"])

    # Load remaining pages
    for offset in range(first["limit"], first["total"], first["limit"]):
        chunk = spotify_session.current_user_playlists(offset=offset)
        playlists.extend(chunk["items"])

    for playlist in playlists:
        tracks = []
        offset = 0
        while True:
            chunk = spotify_session.playlist_tracks(playlist["id"], offset=offset)
            tracks.extend([item["track"] for item in chunk["items"] if item["track"]])
            if not chunk["next"]:
                break
            offset += chunk["limit"]
        playlist_name_lower = playlist["name"].strip().lower()
        result[playlist_name_lower] = {
            "name": playlist["name"],
            "id": playlist["id"],
            "tracks": tracks,
        }
    return result


async def get_spotify_favorite_track_ids(spotify_session: spotipy.Spotify) -> Set[str]:
    favorites = []
    offset = 0
    while True:
        chunk = spotify_session.current_user_saved_tracks(offset=offset)
        favorites.extend(
            [item["track"]["id"] for item in chunk["items"] if item["track"]]
        )
        if not chunk["next"]:
            break
        offset += chunk["limit"]
    return set(favorites)


class SyncReport:
    def __init__(self):
        self.playlists_to_create = []
        self.playlists_to_update = []
        self.favorites_to_add = []
        self.not_found_tracks = []
        self.total_tidal_tracks = 0
        self.matched_tracks = 0

    def add_not_found(self, tidal_track: tidalapi.Track):
        self.not_found_tracks.append(tidal_track)

    def summary(self) -> str:
        lines = ["=" * 60, "TIDAL TO SPOTIFY SYNC REPORT", "=" * 60, ""]

        lines.append(f"Total Tidal tracks processed: {self.total_tidal_tracks}")
        lines.append(f"Matched to Spotify: {self.matched_tracks}")
        lines.append(f"Not found on Spotify: {len(self.not_found_tracks)}")
        lines.append("")

        if self.playlists_to_create:
            lines.append(f"PLAYLISTS TO CREATE: {len(self.playlists_to_create)}")
            for p in self.playlists_to_create:
                lines.append(f"  - {p['name']}: {len(p['new_tracks'])} tracks")
            lines.append("")

        if self.playlists_to_update:
            lines.append(f"PLAYLISTS TO UPDATE: {len(self.playlists_to_update)}")
            for p in self.playlists_to_update:
                lines.append(f"  - {p['name']}: {len(p['new_tracks'])} new tracks")
            lines.append("")

        if self.favorites_to_add:
            lines.append(f"FAVORITES TO ADD: {len(self.favorites_to_add)} tracks")
            lines.append("")

        if self.not_found_tracks:
            lines.append(f"NOT FOUND ON SPOTIFY: {len(self.not_found_tracks)} tracks")
            for t in self.not_found_tracks[:20]:
                artist_names = ", ".join([a.name for a in t.artists])
                lines.append(f"  - {artist_names} - {t.name} (ISRC: {t.isrc or 'N/A'})")
            if len(self.not_found_tracks) > 20:
                lines.append(f"  ... and {len(self.not_found_tracks) - 20} more")
            lines.append("")

        return "\n".join(lines)


async def sync_tidal_playlist_to_spotify(
    tidal_playlist: tidalapi.Playlist,
    spotify_session: spotipy.Spotify,
    spotify_playlists: Mapping[str, List[dict]],
    spotify_favorite_ids: Set[str],
    config: dict,
    report: SyncReport,
    dry_run: bool = False,
):
    tidal_tracks = await get_tidal_playlist_tracks(tidal_playlist)
    if not tidal_tracks:
        return

    report.total_tidal_tracks += len(tidal_tracks)

    playlist_name = tidal_playlist.name
    log(f"→ Processing: {playlist_name} ({len(tidal_tracks)} tracks)")
    playlist_name_lower = playlist_name.strip().lower()
    existing_spotify_playlist_data = spotify_playlists.get(playlist_name_lower, {})
    existing_spotify_playlist = existing_spotify_playlist_data.get("tracks", [])
    existing_spotify_playlist_id = existing_spotify_playlist_data.get("id")
    existing_spotify_track_ids = {t["id"] for t in existing_spotify_playlist if t}

    existing_spotify_isrc_prefixes = set()
    for sp_track in existing_spotify_playlist:
        if sp_track and sp_track.get("external_ids", {}).get("isrc"):
            isrc = sp_track["external_ids"]["isrc"]
            if isrc and len(isrc) >= 11:
                existing_spotify_isrc_prefixes.add(isrc[:11])

    for tidal_track in tidal_tracks:
        artists = [a.name for a in tidal_track.artists]
        unified_cache.store_tidal_track(
            tidal_id=tidal_track.id,
            name=tidal_track.name,
            artists=artists,
            album=tidal_track.album.name if tidal_track.album else None,
            isrc=tidal_track.isrc,
            duration=tidal_track.duration,
            track_num=tidal_track.track_num,
        )

    # Stage 1+2: Check DB for duplicates BEFORE Spotify search (saves API calls)
    precheck_results = {}
    for tidal_track in tidal_tracks:
        artists = [a.name for a in tidal_track.artists]
        db_match = unified_cache.is_duplicate(
            tidal_isrc=tidal_track.isrc,
            tidal_name=tidal_track.name,
            tidal_artists=artists,
        )
        if db_match:
            precheck_results[tidal_track.id] = db_match["spotify_id"]

    search_results = await search_new_tracks_on_spotify(
        spotify_session, tidal_tracks, f"playlist '{playlist_name}'", config
    )

    new_spotify_ids = []
    not_found_tracks = []
    for idx, tidal_track in enumerate(tidal_tracks):
        # Check if we already found a duplicate in DB (Stage 1+2)
        db_spotify_id = precheck_results.get(tidal_track.id)

        # Use DB match if found, otherwise use search result
        spotify_id = db_spotify_id if db_spotify_id else search_results[idx]

        if spotify_id:
            report.matched_tracks += 1
            unified_cache.store_match(
                tidal_id=tidal_track.id,
                spotify_id=spotify_id,
                confidence=1.0,
                method="search",
            )
            tidal_isrc_prefix = (
                tidal_track.isrc[:11]
                if tidal_track.isrc and len(tidal_track.isrc) >= 11
                else tidal_track.isrc
            )

            is_already_in_playlist = spotify_id in existing_spotify_track_ids or (
                tidal_isrc_prefix
                and tidal_isrc_prefix in existing_spotify_isrc_prefixes
            )

            if is_already_in_playlist:
                log(f"  - {tidal_track.name}: already in playlist")
            else:
                new_spotify_ids.append(spotify_id)
        else:
            report.add_not_found(tidal_track)
            not_found_tracks.append(tidal_track)

    if not_found_tracks:
        for tidal_track in not_found_tracks:
            artists = [a.name for a in tidal_track.artists]
            unified_cache.cache_not_found(
                direction="tidal_to_spotify",
                track_id=tidal_track.id,
                track_name=tidal_track.name,
                track_artists=artists,
                isrc=tidal_track.isrc,
            )

    if not new_spotify_ids and not dry_run:
        log(f"No new tracks to add to playlist '{playlist_name}'")
        log(f"✓ Done: {playlist_name}")
        return

    if existing_spotify_playlist:
        if dry_run:
            report.playlists_to_update.append(
                {
                    "name": playlist_name,
                    "existing_count": len(existing_spotify_playlist),
                    "new_tracks": new_spotify_ids,
                }
            )
        else:
            log(
                f"Adding {len(new_spotify_ids)} tracks to existing Spotify playlist '{playlist_name}'"
            )
            if existing_spotify_playlist_id:
                for i in range(0, len(new_spotify_ids), 20):
                    spotify_session.playlist_add_items(
                        existing_spotify_playlist_id, new_spotify_ids[i : i + 20]
                    )
    else:
        if dry_run:
            report.playlists_to_create.append(
                {
                    "name": playlist_name,
                    "new_tracks": new_spotify_ids,
                }
            )
        else:
            log(
                f"Creating new Spotify playlist '{playlist_name}' with {len(new_spotify_ids)} tracks"
            )
            user_id = spotify_session.current_user()["id"]
            new_playlist = spotify_session.user_playlist_create(
                user_id, playlist_name, description=f"Imported from Tidal"
            )
            for i in range(0, len(new_spotify_ids), 20):
                spotify_session.playlist_add_items(
                    new_playlist["id"], new_spotify_ids[i : i + 20]
                )

    log(f"✓ Done: {playlist_name}")


async def sync_tidal_favorites_to_spotify(
    tidal_session: tidalapi.Session,
    spotify_session: spotipy.Spotify,
    spotify_favorite_ids: Set[str],
    config: dict,
    report: SyncReport,
    dry_run: bool = False,
):
    tidal_tracks = await get_tidal_favorites_tracks(tidal_session)
    if not tidal_tracks:
        return

    report.total_tidal_tracks += len(tidal_tracks)

    log("Storing Tidal tracks in cache...")
    for tidal_track in tidal_tracks:
        artists = [a.name for a in tidal_track.artists]
        unified_cache.store_tidal_track(
            tidal_id=tidal_track.id,
            name=tidal_track.name,
            artists=artists,
            album=tidal_track.album.name if tidal_track.album else None,
            isrc=tidal_track.isrc,
            duration=tidal_track.duration,
            track_num=tidal_track.track_num,
        )

    search_results = await search_new_tracks_on_spotify(
        spotify_session, tidal_tracks, "favorites", config
    )

    new_favorite_ids = []
    not_found_tracks = []
    for idx, tidal_track in enumerate(tidal_tracks):
        spotify_id = search_results[idx]
        if spotify_id:
            report.matched_tracks += 1
            unified_cache.store_match(
                tidal_id=tidal_track.id,
                spotify_id=spotify_id,
                confidence=1.0,
                method="search",
            )
            if (
                spotify_id not in spotify_favorite_ids
                and spotify_id not in new_favorite_ids
            ):
                new_favorite_ids.append(spotify_id)
        else:
            report.add_not_found(tidal_track)
            not_found_tracks.append(tidal_track)

    if not_found_tracks:
        log(f"Caching {len(not_found_tracks)} not-found tracks...")
        for tidal_track in not_found_tracks:
            artists = [a.name for a in tidal_track.artists]
            unified_cache.cache_not_found(
                direction="tidal_to_spotify",
                track_id=tidal_track.id,
                track_name=tidal_track.name,
                track_artists=artists,
                isrc=tidal_track.isrc,
            )
        unified_cache.log_not_found_to_file("tidal_to_spotify")
        log(f"Not-found tracks logged to songs_not_found_tidal_to_spotify.txt")

    if not new_favorite_ids:
        log("No new favorites to add")
        return

    if dry_run:
        report.favorites_to_add = new_favorite_ids
    else:
        log(f"Adding {len(new_favorite_ids)} tracks to Spotify favorites")
        for spotify_id in tqdm(new_favorite_ids, desc="Adding to Spotify favorites"):
            spotify_session.current_user_saved_tracks_add([spotify_id])


async def sync_tidal_to_spotify(
    tidal_session: tidalapi.Session,
    spotify_session: spotipy.Spotify,
    config: dict,
    dry_run: bool = False,
    playlist_id: str | None = None,
    sync_favorites: bool = False,
):
    report = SyncReport()

    log("Loading Spotify playlists and favorites...")
    spotify_playlists = await get_all_spotify_playlists_tracks(spotify_session)
    spotify_favorite_ids = await get_spotify_favorite_track_ids(spotify_session)
    log(
        f"Found {len(spotify_playlists)} Spotify playlists, {len(spotify_favorite_ids)} favorites"
    )

    if playlist_id:
        log(f"Loading single Tidal playlist {playlist_id}...")
        tidal_playlist = tidal_session.playlist(playlist_id=playlist_id)
        await sync_tidal_playlist_to_spotify(
            tidal_playlist,
            spotify_session,
            spotify_playlists,
            spotify_favorite_ids,
            config,
            report,
            dry_run,
        )
    else:
        log("Loading Tidal playlists...")
        tidal_playlists = await get_all_playlists(tidal_session.user)
        log(f"Found {len(tidal_playlists)} Tidal playlists")

        for tidal_playlist in tqdm(tidal_playlists, desc="Syncing playlists"):
            await sync_tidal_playlist_to_spotify(
                tidal_playlist,
                spotify_session,
                spotify_playlists,
                spotify_favorite_ids,
                config,
                report,
                dry_run,
            )

        if sync_favorites:
            log("\nSyncing favorites...")
            await sync_tidal_favorites_to_spotify(
                tidal_session,
                spotify_session,
                spotify_favorite_ids,
                config,
                report,
                dry_run,
            )

    # Cleanup: Mark tracks as unmatched if they're no longer in Spotify
    if not dry_run:
        log("\nCleaning up deleted Spotify tracks...")
        current_spotify_ids = set(spotify_favorite_ids)
        for playlist_data in spotify_playlists.values():
            for track in playlist_data.get("tracks", []):
                if track and track.get("id"):
                    current_spotify_ids.add(track["id"])

        cleaned = unified_cache.cleanup_deleted_spotify_tracks(current_spotify_ids)
        if cleaned > 0:
            log(f"Cleaned up {cleaned} deleted Spotify tracks from cache")

    log(report.summary())

    if report.not_found_tracks:
        with open("songs_not_found_tidal_to_spotify.txt", "w", encoding="utf-8") as f:
            for track in report.not_found_tracks:
                artist_names = ", ".join([a.name for a in track.artists])
                f.write(
                    f"{track.id}: {artist_names} - {track.name} (ISRC: {track.isrc or 'N/A'})\n"
                )
        log(f"\nNot found tracks logged to songs_not_found_tidal_to_spotify.txt")

    return report


def sync_tidal_to_spotify_wrapper(
    tidal_session: tidalapi.Session,
    spotify_session: spotipy.Spotify,
    config: dict,
    dry_run: bool = False,
    playlist_id: str | None = None,
    sync_favorites: bool = False,
):
    return asyncio.run(
        sync_tidal_to_spotify(
            tidal_session, spotify_session, config, dry_run, playlist_id, sync_favorites
        )
    )


# Genre categories for clean-favorites
GENRE_CATEGORIES = {
    "house": ["house", "deep house", "progressive house"],
    "techno": ["techno", "tech house", "minimal techno"],
    "edm": [
        "edm",
        "electro house",
        "big room",
        "future bass",
        "happy hardcore",
        "nightcore",
    ],
    "synthwave": ["synthwave", "retrowave", "outrun"],
    "electro": ["electro", "electronic", "idm", "glitch"],
    "trance": ["trance", "psytrance", "goa"],
    "dubstep": ["dubstep", "uk dubstep"],
    "drum and bass": ["drum and bass", "drum & bass", "dnb", "jungle"],
    "rock": ["rock", "classic rock", "alt rock"],
    "metal": ["metal", "heavy metal", "death metal"],
    "punk": ["punk", "punk rock", "pop punk"],
    "grunge": ["grunge", "alternative rock"],
    "pop": ["pop", "dance pop", "power pop"],
    "dance": ["dance", "disco"],
    "hip-hop": ["hip-hop", "hip hop"],
    "rap": ["rap", "freestyle"],
    "trap": ["trap", "trap music"],
    "classical": ["classical", "classical crossover"],
    "soundtrack": [
        "soundtrack",
        "score",
        "film score",
        "movie soundtrack",
        "cinematic",
        "orchestral",
    ],
    "jazz": ["jazz", "bebop", "smooth jazz", "bossa nova", "big band"],
    "soul": ["soul", "neo-soul"],
    "r&b": ["r&b", "contemporary r&b"],
    "indie": ["indie", "indie rock", "indie pop"],
    "alternative": ["alternative", "alternative rock"],
    "downtempo": ["downtempo", "trip hop", "lo-fi beats", "slowcore"],
    "ambient": [
        "ambient",
        "ambient music",
        "chillout",
        "chill",
        "electro chill",
        "electro chillout",
        "electronic chillout",
        "electronic chill",
        "ambient electronic",
        "chill electronic",
    ],
    "lounge": ["lounge", "lounge music"],
    "country": ["country", "country pop"],
    "folk": ["folk", "folk rock", "singer-songwriter", "celtic"],
    "americana": ["americana", "alt country"],
    "other": [],
}

ARTIST_NAME_CATEGORIES = {
    "house": [
        "bedroom",
        "deeper",
        "deep",
        "house",
        "filey",
        "monophonic",
        "yotto",
        "eelke",
        "kling",
        "lulu",
    ],
    "techno": ["techno", "tech house"],
    "edm": ["edm", "electro", "kygo", "martin garrix", "zeds dead", " excision"],
    "electro": ["electronic", "synth", "modular", "four tet", "wolf alice"],
    "ambient": [
        "bonobo",
        "tycho",
        "christian loffler",
        "mahmut orhan",
        "st germain",
        "bugge wessel",
        "dj koze",
        "sasha",
        "john digweed",
        "lane 8",
        "kiasmos",
        "moderat",
        "nicolas jaar",
        "parra for cuva",
        "lapalux",
        "rival consoles",
        "monolink",
        "blockhead",
        "thievery corporation",
        "nocando",
    ],
    "dubstep": ["dubstep", "skrillex"],
    "drum and bass": ["drum", "dnb", "netsky"],
    "rock": [
        "coldplay",
        "the fray",
        "snow patrol",
        "ash",
        "radiohead",
        "u2",
        "queen",
        "foo fighters",
        "nin",
        "tool",
        "deftones",
        "museum",
        "the national",
        "mumford",
        "imagine dragons",
    ],
    "metal": ["metal", "metallic", "slipknot", "mastodon", "linkin park"],
    "pop": [
        "taylor swift",
        "ed sheeran",
        "billie eilish",
        "dua lipa",
        "harry styles",
        "bruno mars",
        "justin bieber",
        "ariana grande",
        "weekend",
        "niall horan",
        "dean lewis",
        "james bay",
        "katy perry",
        "rihanna",
        "sam smith",
        "sia",
        "adele",
    ],
    "hip-hop": ["eminem", "kendrick", "drake", "jay-z", "kanye", "kendrick lamar"],
    "rap": ["rap"],
    "classical": ["classical", "bach", "mozart", "beethoven", "vivaldi", "chopin"],
    "soundtrack": [
        "hans zimmer",
        "john williams",
        "ost",
        "soundtrack",
        "ramin djawadi",
        "michael giacchino",
        "howard shore",
        "thomas newman",
        "james newton howard",
        "danny elfman",
    ],
    "jazz": ["jazz", "miles davis", "coltrane", "norah jones", "laptop", "floyd"],
    "indie": [
        "indie",
        "the 1975",
        "arctic monkeys",
        "flume",
        "odesza",
        "bicep",
        "rÜfÜs du sol",
        "rufus du sol",
        "kakkmaddafakka",
        "half moon run",
        "glass animals",
        " foster the people",
        "m83",
    ],
    "folk": [
        "ed sheeran",
        "passenger",
        "john mayer",
        "bob dylan",
        "joni",
        "james blake",
    ],
    "singer-songwriter": ["singer", "songwriter"],
    "downtempo": ["blocks", "ruis"],
}


def map_spotify_genre_to_category(spotify_genres: list, artist_name: str = "") -> str:
    """Map Spotify genre tags to our categories"""
    for category, keywords in GENRE_CATEGORIES.items():
        for spotify_genre in spotify_genres:
            spotify_genre_lower = spotify_genre.lower()
            if category == spotify_genre_lower:
                return category.upper()
            for kw in keywords:
                if kw in spotify_genre_lower or spotify_genre_lower in kw:
                    return category.upper()

    if artist_name:
        artist_lower = artist_name.lower()
        for category, name_keywords in ARTIST_NAME_CATEGORIES.items():
            for kw in name_keywords:
                if kw in artist_lower:
                    return category.upper()

    return "OTHER"


def map_audio_features_to_category(audio_features: dict) -> str:
    """Map Spotify audio features to genre category"""
    if not audio_features:
        return "OTHER"

    tempo = audio_features.get("tempo", 0)
    energy = audio_features.get("energy", 0)
    danceability = audio_features.get("danceability", 0)
    valence = audio_features.get("valence", 0)
    acousticness = audio_features.get("acousticness", 0)
    instrumentalness = audio_features.get("instrumentalness", 0)

    if instrumentalness > 0.5:
        if tempo < 100:
            return "AMBIENT"
        return "SOUNDTRACK"

    if tempo > 170 and energy > 0.7:
        return "DRUM AND BASS"

    if tempo > 120 and energy > 0.7:
        if danceability > 0.7:
            return "EDM"
        return "ROCK"

    if 110 <= tempo <= 130 and energy > 0.6:
        if danceability > 0.6:
            return "HOUSE"
        if instrumentalness > 0.1:
            return "SOUNDTRACK"

    if tempo > 140 and energy > 0.5:
        return "TECHNO"

    if energy < 0.4 and acousticness > 0.5:
        return "FOLK"

    if energy < 0.4 and (valence > 0.5 or danceability < 0.4):
        return "POP"

    if energy < 0.5 and instrumentalness < 0.3:
        return "INDIE"

    if danceability > 0.7 and energy > 0.5:
        return "DANCE"

    return "OTHER"


MUSICBRAINZ_GENRE_MAP = {
    "rock": "rock",
    "pop": "pop",
    "electronic": "electro",
    "dance": "dance",
    "edm": "edm",
    "classical": "classical",
    "soundtrack": "soundtrack",
    "ambient": "ambient",
    "jazz": "jazz",
    "folk": "folk",
    "hip-hop": "hip-hop",
    "hip hop": "hip-hop",
    "metal": "metal",
    "indie": "indie",
    "alternative": "alternative",
    "punk": "punk",
    "reggae": "other",
    "blues": "jazz",
    "soul": "soul",
    "r&b": "r&b",
    "rnb": "r&b",
    "country": "country",
    "latin": "other",
    "world": "other",
    "funk": "dance",
    "disco": "dance",
    "house": "house",
    "techno": "techno",
    "trance": "trance",
    "dubstep": "dubstep",
    "drum and bass": "drum and bass",
    "lo-fi": "downtempo",
    "lofi": "downtempo",
    "chill": "ambient",
    "experimental": "electro",
    "noise": "electro",
    "sound art": "soundtrack",
    "score": "soundtrack",
    "cinematic": "soundtrack",
    "orchestral": "soundtrack",
    "instrumental": "ambient",
    "piano": "classical",
    "singer-songwriter": "folk",
    "acoustic": "folk",
}


def load_genre_fallback_map():
    """Load genre fallback CSV for OTHER artists"""
    import csv
    import os

    fallback_map = {}
    csv_path = os.path.join(os.path.dirname(__file__), "..", "..", "genre_fallback.csv")

    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                artist_name = row.get("artist_name", "").strip()
                genre = row.get("genre", "").strip()
                if artist_name and genre:
                    fallback_map[artist_name] = genre
    except FileNotFoundError:
        pass

    return fallback_map


_genre_fallback_cache = None


def get_genre_fallback(artist_name: str) -> str | None:
    """Get genre from fallback CSV for an artist"""
    global _genre_fallback_cache

    if _genre_fallback_cache is None:
        _genre_fallback_cache = load_genre_fallback_map()

    return _genre_fallback_cache.get(artist_name)


def save_other_artists_list(other_artists: list):
    """Save OTHER artists to a list for later processing by LLM"""
    import csv
    import os

    csv_path = os.path.join(
        os.path.dirname(__file__), "..", "..", "other_artists_new.csv"
    )

    existing = set()
    if os.path.exists(csv_path):
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                existing.add(row.get("artist_name", "").strip())

    new_artists = [a for a in other_artists if a not in existing]

    if new_artists:
        with open(csv_path, "a", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            if f.tell() == 0:
                writer.writerow(["artist_name", "genre"])
            for artist in new_artists:
                writer.writerow([artist, ""])

        log(f"\nSaved {len(new_artists)} new OTHER artists to other_artists_new.csv")
        log("Run 'other_artists_new.csv' through LLM to assign genres")


def map_musicbrainz_genres_to_category(musicbrainz_genres: list) -> str:
    """Map MusicBrainz genres to our categories"""
    if not musicbrainz_genres:
        return "OTHER"

    top_genre = (
        musicbrainz_genres[0].get("name", "").lower() if musicbrainz_genres else ""
    )
    if not top_genre:
        return "OTHER"

    if top_genre in MUSICBRAINZ_GENRE_MAP:
        return MUSICBRAINZ_GENRE_MAP[top_genre].upper()

    for category, mb_genres in {
        "rock": [
            "rock",
            "alternative rock",
            "hard rock",
            "classic rock",
            "indie rock",
            "punk",
            "metal",
            "grunge",
        ],
        "pop": ["pop", "dance pop", "synth pop", "electropop"],
        "electro": ["electronic", "experimental", "idm", "glitch", "synth"],
        "house": ["house", "deep house", "progressive house", "tech house"],
        "techno": ["techno", "minimal techno"],
        "edm": ["edm", "electro house", "big room"],
        "dance": ["dance", "disco", "funk"],
        "classical": ["classical", "contemporary classical", "baroque", "piano"],
        "soundtrack": ["soundtrack", "score", "film score", "video game music"],
        "ambient": [
            "ambient",
            "drone",
            "new age",
            "chillout",
            "electronic chillout",
            "ambient electronic",
        ],
        "jazz": ["jazz", "smooth jazz", "bebop", "swing"],
        "folk": ["folk", "singer-songwriter", "acoustic", "americana"],
        "hip-hop": ["hip-hop", "hip hop", "rap"],
    }.items():
        if any(g in top_genre for g in mb_genres):
            return category.upper()

    return "OTHER"


_musicbrainz_session = None


def get_musicbrainz_client():
    """Get or create MusicBrainz client"""
    global _musicbrainz_session
    if _musicbrainz_session is None:
        import musicbrainzngs

        musicbrainzngs.set_useragent(
            "music-sync", "1.0", "https://github.com/stefan-weitzel/music-sync"
        )
        _musicbrainz_session = musicbrainzngs
    return _musicbrainz_session


def lookup_artist_genre_musicbrainz(artist_name: str) -> tuple[str, list] | None:
    """Look up artist genre from MusicBrainz. Returns (category, mb_genres) or None"""
    import time

    mb = get_musicbrainz_client()

    try:
        result = mb.search_artists(artist=artist_name, limit=1)
        artists = result.get("artist-list", [])

        if not artists:
            return None

        artist = artists[0]
        mbid = artist.get("id")

        if not mbid:
            return None

        time.sleep(1.1)

        try:
            artist_data = mb.get_artist_by_id(mbid, includes=["tags"])
            mb_genres = artist_data.get("artist", {}).get("tag-list", [])

            genre_names = []
            for g in mb_genres:
                name = g.get("name", "")
                count = g.get("count", 1)
                genre_names.append({"name": name, "count": count})

            if not genre_names:
                return None

            category = map_musicbrainz_genres_to_category(genre_names)

            return (category, genre_names)
        except Exception as e:
            log(f"MusicBrainz lookup error for {artist_name}: {e}")
            return None

    except Exception as e:
        log(f"MusicBrainz search error for {artist_name}: {e}")
        return None


def preload_spotify_data(spotify_session: spotipy.Spotify) -> dict:
    """Pre-load all Spotify data: favorites + playlists + ISRCs"""
    log("Loading all Spotify data...")

    # Load favorites
    favorites_isrcs = set()
    favorites = []
    offset = 0
    while True:
        chunk = spotify_session.current_user_saved_tracks(offset=offset)
        for item in chunk["items"]:
            if item["track"]:
                track = item["track"]
                favorites.append(track)
                isrc = track.get("external_ids", {}).get("isrc")
                if isrc:
                    favorites_isrcs.add(isrc)
        if not chunk["next"]:
            break
        offset += chunk["limit"]

    # Load all playlists with their tracks
    playlists_data = {}
    user_playlists = spotify_session.current_user_playlists()
    all_playlists_isrcs = set()

    for pl in user_playlists["items"]:
        pl_id = pl["id"]
        pl_name = pl["name"]
        pl_tracks = []

        offset = 0
        while True:
            chunk = spotify_session.playlist_tracks(pl_id, offset=offset)
            for item in chunk["items"]:
                if item["track"]:
                    track = item["track"]
                    pl_tracks.append(track)
                    isrc = track.get("external_ids", {}).get("isrc")
                    if isrc:
                        all_playlists_isrcs.add(isrc)
            if not chunk["next"]:
                break
            offset += chunk["limit"]

        playlists_data[pl_name.lower()] = {
            "id": pl_id,
            "name": pl_name,
            "tracks": pl_tracks,
            "isrcs": {
                t.get("external_ids", {}).get("isrc")
                for t in pl_tracks
                if t.get("external_ids", {}).get("isrc")
            },
        }

    return {
        "favorites": favorites,
        "favorites_isrcs": favorites_isrcs,
        "playlists": playlists_data,
        "all_isrcs": favorites_isrcs | all_playlists_isrcs,
    }


async def clean_playlist(
    spotify_session: spotipy.Spotify,
    tidal_session,
    playlist_uri: str | None,
    clean_source: str = "spotify",
    dry_run: bool = False,
):
    """Organize source into genre-based playlists on the same platform"""

    # For --clean: destination is the SAME as source (stay on same platform)
    # clean_source="spotify" → stay on Spotify → create on Spotify → tidal_to_spotify=True
    # clean_source="tidal" → stay on Tidal → create on Tidal → tidal_to_spotify=False
    tidal_to_spotify = clean_source == "spotify"

    # Pre-load all Spotify data (API calls once)
    spotify_data = await asyncio.to_thread(preload_spotify_data, spotify_session)
    all_existing_isrcs = spotify_data["all_isrcs"]
    favorites_isrcs = spotify_data["favorites_isrcs"]
    playlists_data = spotify_data["playlists"]

    is_favorites = playlist_uri is None
    source_name = "favorites" if is_favorites else "playlist"

    log(f"Loading Spotify source: {source_name}...")
    source_tracks = []
    playlist_name = "favorites"

    if is_favorites:
        offset = 0
        while True:
            chunk = spotify_session.current_user_saved_tracks(offset=offset)
            source_tracks.extend(
                [item["track"] for item in chunk["items"] if item["track"]]
            )
            if not chunk["next"]:
                break
            offset += chunk["limit"]
    else:
        playlist_id = playlist_uri.split(":")[-1]
        playlist = spotify_session.playlist(playlist_id)
        playlist_name = playlist["name"]
        offset = 0
        while True:
            chunk = spotify_session.playlist_tracks(playlist_id, offset=offset)
            source_tracks.extend(
                [item["track"] for item in chunk["items"] if item["track"]]
            )
            if not chunk["next"]:
                break
            offset += chunk["limit"]

    log(f"Found {len(source_tracks)} tracks in {source_name}")

    genre_tracks = defaultdict(list)
    genre_artists = defaultdict(set)
    track_artist_info = {}
    track_isrc = {}

    log("Categorizing tracks by genre...")
    processed_artists = set()

    for track in tqdm(source_tracks, desc="Categorizing"):
        artist_id = track["artists"][0]["id"]
        artist_name = track["artists"][0]["name"]
        track_id = track["id"]
        isrc = track.get("external_ids", {}).get("isrc", "")

        track_artist_info[track_id] = {
            "artist_id": artist_id,
            "artist_name": artist_name,
        }
        track_isrc[track_id] = isrc

        # Check ARTIST_NAME_CATEGORIES FIRST before using cached genre
        name_based_genre = map_spotify_genre_to_category([], artist_name)

        cached_genre = unified_cache.get_artist_genre(artist_id)

        # Use name-based genre if found, otherwise use cache, otherwise OTHER
        if name_based_genre and name_based_genre != "OTHER":
            genre = name_based_genre
        elif cached_genre:
            genre = cached_genre
        elif artist_id in processed_artists:
            genre = "OTHER"
        else:
            genre = "OTHER"

        # Update cache with new genre if different from cached
        if (
            name_based_genre
            and name_based_genre != "OTHER"
            and name_based_genre != cached_genre
        ):
            unified_cache.store_artist_genre(
                artist_id, artist_name, name_based_genre, []
            )
            processed_artists.add(artist_id)

        genre_tracks[genre].append(track_id)
        genre_artists[genre].add(artist_name)

    other_artists = list(genre_artists.get("OTHER", set()))

    if other_artists:
        for artist_name in tqdm(other_artists, desc="MusicBrainz"):
            if not artist_name or not artist_name.strip():
                continue

            artist_id = None
            for track_id, info in track_artist_info.items():
                if info["artist_name"] == artist_name:
                    artist_id = info["artist_id"]
                    break

            if artist_id:
                result = lookup_artist_genre_musicbrainz(artist_name)
                if result:
                    category, mb_genres = result
                    unified_cache.store_artist_genre_musicbrainz(
                        artist_id, artist_name, category, mb_genres
                    )
                else:
                    unified_cache.store_artist_genre_musicbrainz(
                        artist_id, artist_name, "OTHER", []
                    )

        new_genre_tracks = defaultdict(list)
        new_genre_artists = defaultdict(set)

        for track_id, info in track_artist_info.items():
            artist_id = info["artist_id"]
            artist_name = info["artist_name"]

            cached_genre = unified_cache.get_artist_genre(artist_id)
            if not cached_genre or cached_genre == "OTHER":
                fallback_genre = get_genre_fallback(artist_name)
                if fallback_genre:
                    cached_genre = fallback_genre
            genre = cached_genre if cached_genre else "OTHER"

            new_genre_tracks[genre].append(track_id)
            new_genre_artists[genre].add(artist_name)

        genre_tracks = new_genre_tracks
        genre_artists = new_genre_artists

        new_other_artists = list(new_genre_artists.get("OTHER", set()))
        if new_other_artists:
            save_other_artists_list(new_other_artists)

    if dry_run:
        log("\n=== DRY RUN - No changes made ===")
        return

    if tidal_to_spotify:
        log("\nCreating/updating genre playlists on Spotify...")
        user_id = spotify_session.current_user()["id"]

        for genre, track_ids in genre_tracks.items():
            playlist_name_clean = f"{playlist_name}-{genre}"

            # Use pre-loaded playlists data (no API call)
            existing_playlist_data = playlists_data.get(playlist_name_clean.lower())

            if existing_playlist_data:
                playlist_id = existing_playlist_data["id"]
                existing_isrcs = existing_playlist_data["isrcs"]
                log(
                    f"Found existing playlist: {playlist_name_clean} ({len(existing_isrcs)} tracks)"
                )
            else:
                log(f"Creating playlist: {playlist_name_clean}")
                new_playlist = spotify_session.user_playlist_create(
                    user_id, playlist_name_clean, description=f"Genre: {genre}"
                )
                playlist_id = new_playlist["id"]
                existing_isrcs = set()

            new_track_ids = []
            duplicate_exact = 0
            duplicate_prefix = 0

            for track_id in track_ids:
                # Get track from source_tracks (no API call)
                track_info = next(
                    (t for t in source_tracks if t["id"] == track_id), None
                )
                if not track_info:
                    continue

                isrc = track_info.get("external_ids", {}).get("isrc")

                # Duplicate detection
                if isrc:
                    # Check exact match
                    if isrc in all_existing_isrcs:
                        duplicate_exact += 1
                        continue

                    # Check prefix (remaster)
                    if len(isrc) >= 11:
                        prefix = isrc[:11]
                        if any(
                            prefix == existing_isrc[:11]
                            for existing_isrc in all_existing_isrcs
                            if existing_isrc and len(existing_isrc) >= 11
                        ):
                            duplicate_prefix += 1

                    all_existing_isrcs.add(isrc)

                if isrc and isrc not in existing_isrcs:
                    new_track_ids.append(track_id)
                    existing_isrcs.add(isrc)

            if new_track_ids:
                for i in range(0, len(new_track_ids), 20):
                    spotify_session.playlist_add_items(
                        playlist_id, new_track_ids[i : i + 20]
                    )
    else:
        log("\nMatching tracks to Tidal...")

        from .matcher import matcher
        from .tidalapi_patch import add_multiple_tracks_to_playlist

        genre_tidal_tracks = defaultdict(list)

        log("Searching Tidal for tracks...")
        search_semaphore = asyncio.Semaphore(3)

        async def search_tidal_track(track_id, isrc, track_name, artists):
            await search_semaphore.acquire()
            try:
                query = f"{track_name} {' '.join(artists)}"
                search_results = tidal_session.search(
                    query, models=[tidalapi.media.Track], limit=10
                )["tracks"]
                for result in search_results:
                    if result.isrc == isrc:
                        return result.id
                return None
            finally:
                search_semaphore.release()

        tracks_to_search = []
        for track_id, isrc in track_isrc.items():
            if isrc:
                info = track_artist_info[track_id]
                artists = [info["artist_name"]]
                for track in source_tracks:
                    if track["id"] == track_id:
                        artists = [a["name"] for a in track["artists"]]
                        break
                tracks_to_search.append((track_id, isrc, track["name"], artists))

        tidal_track_ids = {}
        for track_id, isrc, track_name, artists in tqdm(
            tracks_to_search, desc="Searching Tidal"
        ):
            await asyncio.sleep(0.5)
            try:
                result = await search_tidal_track(track_id, isrc, track_name, artists)
                if result:
                    tidal_track_ids[track_id] = result
            except Exception as e:
                log(f"Search error for {track_name}: {e}")

        for genre, track_ids in genre_tracks.items():
            for track_id in track_ids:
                if track_id in tidal_track_ids:
                    genre_tidal_tracks[genre].append(tidal_track_ids[track_id])

        log("\nCreating/updating genre playlists on Tidal...")
        user = tidal_session.user

        for genre, tidal_track_ids_list in genre_tidal_tracks.items():
            playlist_name_clean = f"{playlist_name}-{genre}"

            existing_playlists = await get_all_playlists(user)
            existing_playlist = None
            for p in existing_playlists:
                if p.name == playlist_name_clean:
                    existing_playlist = p
                    break

            if existing_playlist:
                log(f"Found existing playlist: {playlist_name_clean}")
                add_multiple_tracks_to_playlist(existing_playlist, tidal_track_ids_list)
            else:
                log(f"Creating playlist: {playlist_name_clean}")
                new_playlist = user.playlist_create(
                    playlist_name_clean, f"Genre: {genre}"
                )
                add_multiple_tracks_to_playlist(new_playlist, tidal_track_ids_list)

        log(f"\nMatched {len(tidal_track_ids)} of {len(source_tracks)} tracks to Tidal")
        not_matched = len(source_tracks) - len(tidal_track_ids)
        if not_matched > 0:
            log(f"Could not match {not_matched} tracks")

    total_in_genres = sum(len(tracks) for tracks in genre_tracks.values())
    log("\n=== CLEAN ERFOLGREICH ===")
    log(f"Alle {len(source_tracks)} Tracks wurden in Genre-Playlists organisiert.")
    if total_in_genres != len(source_tracks):
        log(
            f"⚠️ WARNUNG: {len(source_tracks)} Quell-Tracks, aber {total_in_genres} in Genres!"
        )
    else:
        log(f"✓ Verification OK: {total_in_genres} Tracks verteilt")
    log("")
    log("Bitte lösche die Quelle manuell in Spotify:")
    log("→ Bibliothek → Lieblingssongs (für Favorites)")
    log("→ Playlist öffnen → ⋮ → Von Bibliothek entfernen (für Playlists)")


def clean_playlist_wrapper(
    spotify_session: spotipy.Spotify,
    tidal_session,
    playlist_uri: str | None = None,
    clean_source: str = "spotify",
    dry_run: bool = False,
):
    return asyncio.run(
        clean_playlist(
            spotify_session, tidal_session, playlist_uri, clean_source, dry_run
        )
    )
