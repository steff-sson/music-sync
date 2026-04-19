import yaml
import argparse
import sys

from . import sync as _sync
from . import auth as _auth


def main():
    parser = argparse.ArgumentParser()
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
        "--dry-run",
        action="store_true",
        help="show what would be done without making changes",
    )
    args = parser.parse_args()

    with open(args.config, "r") as f:
        config = yaml.safe_load(f)

    if args.tidal_to_spotify:
        print("Opening Tidal session")
        tidal_session = _auth.open_tidal_session()
        if not tidal_session.check_login():
            sys.exit("Could not connect to Tidal")
        print("Opening Spotify session")
        spotify_session = _auth.open_spotify_session(config["spotify"])

        print(
            f"\n=== TIDAL TO SPOTIFY SYNC ({'DRY RUN' if args.dry_run else 'LIVE'}) ===\n"
        )
        _sync.sync_tidal_to_spotify_wrapper(
            tidal_session, spotify_session, config, dry_run=args.dry_run
        )
    else:
        print("Opening Spotify session")
        spotify_session = _auth.open_spotify_session(config["spotify"])
        print("Opening Tidal session")
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


if __name__ == "__main__":
    main()
    sys.exit(0)
