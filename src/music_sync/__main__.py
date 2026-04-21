import yaml
import argparse
import sys
import logging
from datetime import datetime

from . import sync as _sync
from . import auth as _auth
from .sync_engine import sync_engine


def setup_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.handlers = []

    file_handler = logging.FileHandler("music-sync.log", mode="a")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    )
    logger.addHandler(file_handler)


def log(*args, **kwargs):
    msg = " ".join(str(a) for a in args)
    print(msg, **kwargs)  # Note: Using print() here, not log() to avoid recursion
    logging.info(msg)


def main():
    setup_logging()
    logging.info("=" * 50)
    logging.info(f"music-sync started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info("=" * 50)

    parser = argparse.ArgumentParser(
        description="music-sync - Bidirectional sync between Spotify and Tidal"
    )
    parser.add_argument(
        "--config", default="config.yml", help="location of the config file"
    )
    parser.add_argument(
        "--uri", help="synchronize a specific URI instead of the one in the config"
    )
    parser.add_argument(
        "--sync-favorites",
        action=argparse.BooleanOptionalAction,
        help="synchronize the favorites",
    )
    parser.add_argument(
        "--tidal-to-spotify",
        action="store_true",
        help="sync from Tidal to Spotify instead of Spotify to Tidal",
    )
    parser.add_argument(
        "--spotify-to-tidal",
        action="store_true",
        help="sync from Spotify to Tidal (default behavior)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="show what would be done without making changes",
    )
    parser.add_argument(
        "--force-refresh",
        action="store_true",
        help="ignore cache and refresh all matches",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="organize source into genre-based playlists and remove from source (requires direction)",
    )

    parser.add_argument(
        "direction",
        nargs="*",
        help="direction: tidal spotify OR spotify tidal",
    )

    args = parser.parse_args()

    direction = args.direction if hasattr(args, "direction") else []

    with open(args.config, "r") as f:
        config = yaml.safe_load(f)

    if len(direction) == 2:
        input_platform, output_platform = direction
        if input_platform == "tidal" and output_platform == "spotify":
            args.tidal_to_spotify = True
        elif input_platform == "spotify" and output_platform == "tidal":
            args.tidal_to_spotify = False
        else:
            sys.exit(
                f"Invalid direction: {input_platform} {output_platform}. Use 'tidal spotify' or 'spotify tidal'"
            )

    with open(args.config, "r") as f:
        config = yaml.safe_load(f)

    if args.clean:
        if not hasattr(args, 'tidal_to_spotify') or args.tidal_to_spotify is None:
            sys.exit("--clean requires a direction: 'tidal spotify' or 'spotify tidal'")
        
        spotify_session = _auth.open_spotify_session(config["spotify"])
        tidal_session = None
        
        if args.tidal_to_spotify:
            tidal_session = _auth.open_tidal_session()
            if not tidal_session.check_login():
                sys.exit("Could not connect to Tidal")
        else:
            tidal_session = _auth.open_tidal_session()
            if not tidal_session.check_login():
                sys.exit("Could not connect to Tidal")
        
        if not args.dry_run and not args.uri:
            confirm = input(
                "⚠️  This will DELETE all your favorites after organizing into playlists.\n"
                "    Are you sure? (yes/no): "
            )
            if confirm.lower() != "yes":
                log("Aborted.")
                sys.exit(0)
        
        log(
            f"\n=== CLEAN ({'DRY RUN' if args.dry_run else 'LIVE'}) ===\n"
        )
        _sync.clean_playlist_wrapper(
            spotify_session, 
            tidal_session, 
            playlist_uri=args.uri,
            tidal_to_spotify=args.tidal_to_spotify,
            dry_run=args.dry_run
        )
        return

    if args.tidal_to_spotify:
        log("Opening Tidal session")
        tidal_session = _auth.open_tidal_session()
        if not tidal_session.check_login():
            sys.exit("Could not connect to Tidal")
        log("Opening Spotify session")
        spotify_session = _auth.open_spotify_session(config["spotify"])

        log(
            f"\n=== TIDAL TO SPOTIFY SYNC ({'DRY RUN' if args.dry_run else 'LIVE'}) ===\n"
        )
        _sync.sync_tidal_to_spotify_wrapper(
            tidal_session, spotify_session, config, dry_run=args.dry_run, playlist_id=args.uri, sync_favorites=args.sync_favorites
        )
    else:
        log("Opening Spotify session")
        spotify_session = _auth.open_spotify_session(config["spotify"])
        log("Opening Tidal session")
        tidal_session = _auth.open_tidal_session()
        if not tidal_session.check_login():
            sys.exit("Could not connect to Tidal")
        if args.uri:
            spotify_playlist = spotify_session.playlist(args.uri)
            tidal_playlists = _sync.get_tidal_playlists_wrapper(tidal_session)
            tidal_playlist = _sync.pick_tidal_playlist_for_spotify_playlist(
                spotify_playlist, tidal_playlists
            )
            _sync.sync_playlists_wrapper(
                spotify_session, tidal_session, [tidal_playlist], config
            )
            sync_favorites = args.sync_favorites
        elif args.sync_favorites:
            sync_favorites = True
        elif config.get("sync_playlists", None):
            _sync.sync_playlists_wrapper(
                spotify_session,
                tidal_session,
                _sync.get_playlists_from_config(spotify_session, tidal_session, config),
                config,
            )
            sync_favorites = args.sync_favorites is None and config.get(
                "sync_favorites_default", True
            )
        else:
            _sync.sync_playlists_wrapper(
                spotify_session,
                tidal_session,
                _sync.get_user_playlist_mappings(
                    spotify_session, tidal_session, config
                ),
                config,
            )
            sync_favorites = args.sync_favorites is None and config.get(
                "sync_favorites_default", True
            )

        if sync_favorites:
            _sync.sync_favorites_wrapper(spotify_session, tidal_session, config)

    logging.info("=" * 50)
    logging.info(f"music-sync finished at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logging.info("=" * 50)


if __name__ == "__main__":
    main()
    sys.exit(0)
