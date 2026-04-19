#!/usr/bin/env python3

import asyncio
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
            print(f"{str(e)} occurred, retrying {remaining} times")
        else:
            print(f"{str(e)} could not be recovered")

        retry_after = None
        if isinstance(e, spotipy.exceptions.SpotifyException):
            if e.http_status == 429:
                print(f"Spotify rate limit hit (429)")
                if "rate_limit_state" in globals():
                    rate_limit_state["consecutive_429s"] += 1
                    backoff_seconds = min(
                        30 * (2 ** rate_limit_state["consecutive_429s"]), 300
                    )
                    now = time.time()
                    rate_limit_state["cooldown_until"] = now + backoff_seconds
                    print(f"Entering cooldown for {backoff_seconds}s")
        elif (
            isinstance(e, requests.exceptions.RequestException)
            and e.response is not None
        ):
            print(f"Response message: {e.response.text}")
            print(f"Response headers: {e.response.headers}")
            retry_after = e.response.headers.get("Retry-After")

        if not remaining:
            print("Aborting sync")
            print(f"The following arguments were provided:\n\n {str(args)}")
            print(traceback.format_exc())
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

    print(f"Loading tracks from Spotify playlist '{spotify_playlist['name']}'")
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
                print(
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
            print(color[0] + "Could not find the track " + song404[-1] + color[1])
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
        print(
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
        print("No changes to write to Tidal playlist")
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

    print("Loading favorite tracks from Spotify")
    spotify_tracks = await get_tracks_from_spotify_favorites()
    print("Loading existing favorite tracks from Tidal")
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
        print("No new tracks to add to Tidal favorites")


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
    print("Loading Spotify playlists")
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
            print(f"Error getting Spotify playlist {spotify_id}")
            raise e
        try:
            tidal_playlist = tidal_session.playlist(playlist_id=tidal_id)
        except Exception as e:
            print(f"Error getting Tidal playlist {tidal_id}")
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
        print(
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
        print(f"Skipping {skipped} previously failed tracks")

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
    playlists = spotify_session.current_user_playlists()
    for playlist in playlists["items"]:
        tracks = []
        offset = 0
        while True:
            chunk = spotify_session.playlist_tracks(playlist["id"], offset=offset)
            tracks.extend([item["track"] for item in chunk["items"] if item["track"]])
            if not chunk["next"]:
                break
            offset += chunk["limit"]
        result[playlist["name"]] = tracks
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
    existing_spotify_playlist = spotify_playlists.get(playlist_name, [])
    existing_spotify_track_ids = {t["id"] for t in existing_spotify_playlist if t}

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
        spotify_session, tidal_tracks, f"playlist '{playlist_name}'", config
    )

    new_spotify_ids = []
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
            if spotify_id not in existing_spotify_track_ids:
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
        print(f"No new tracks to add to playlist '{playlist_name}'")
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
            print(
                f"Adding {len(new_spotify_ids)} tracks to existing Spotify playlist '{playlist_name}'"
            )
            user_id = spotify_session.current_user()["id"]
            spotify_playlist_id = None
            for p in spotify_session.current_user_playlists()["items"]:
                if p["name"] == playlist_name:
                    spotify_playlist_id = p["id"]
                    break
            if spotify_playlist_id:
                for i in range(0, len(new_spotify_ids), 20):
                    spotify_session.playlist_add_items(
                        spotify_playlist_id, new_spotify_ids[i : i + 20]
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
            print(
                f"Creating new Spotify playlist '{playlist_name}' with {len(new_spotify_ids)} tracks"
            )
            new_playlist = spotify_session.user_playlist_create(
                playlist_name, description=f"Imported from Tidal"
            )
            for i in range(0, len(new_spotify_ids), 20):
                spotify_session.playlist_add_items(
                    new_playlist["id"], new_spotify_ids[i : i + 20]
                )


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

    print("Storing Tidal tracks in cache...")
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
        print(f"Caching {len(not_found_tracks)} not-found tracks...")
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
        print(f"Not-found tracks logged to songs_not_found_tidal_to_spotify.txt")

    if not new_favorite_ids:
        print("No new favorites to add")
        return

    if dry_run:
        report.favorites_to_add = new_favorite_ids
    else:
        print(f"Adding {len(new_favorite_ids)} tracks to Spotify favorites")
        for spotify_id in tqdm(new_favorite_ids, desc="Adding to Spotify favorites"):
            spotify_session.current_user_saved_tracks_add([spotify_id])


async def sync_tidal_to_spotify(
    tidal_session: tidalapi.Session,
    spotify_session: spotipy.Spotify,
    config: dict,
    dry_run: bool = False,
):
    report = SyncReport()

    print("Loading Tidal playlists...")
    tidal_playlists = await get_all_playlists(tidal_session.user)
    print(f"Found {len(tidal_playlists)} Tidal playlists")

    print("Loading Spotify playlists and favorites...")
    spotify_playlists = await get_all_spotify_playlists_tracks(spotify_session)
    spotify_favorite_ids = await get_spotify_favorite_track_ids(spotify_session)
    print(
        f"Found {len(spotify_playlists)} Spotify playlists, {len(spotify_favorite_ids)} favorites"
    )

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

    print("\nSyncing favorites...")
    await sync_tidal_favorites_to_spotify(
        tidal_session, spotify_session, spotify_favorite_ids, config, report, dry_run
    )

    print(report.summary())

    if report.not_found_tracks:
        with open("songs_not_found_tidal_to_spotify.txt", "w", encoding="utf-8") as f:
            for track in report.not_found_tracks:
                artist_names = ", ".join([a.name for a in track.artists])
                f.write(
                    f"{track.id}: {artist_names} - {track.name} (ISRC: {track.isrc or 'N/A'})\n"
                )
        print(f"\nNot found tracks logged to songs_not_found_tidal_to_spotify.txt")

    return report


def sync_tidal_to_spotify_wrapper(
    tidal_session: tidalapi.Session,
    spotify_session: spotipy.Spotify,
    config: dict,
    dry_run: bool = False,
):
    return asyncio.run(
        sync_tidal_to_spotify(tidal_session, spotify_session, config, dry_run)
    )
