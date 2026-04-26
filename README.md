# music-sync

A command line tool for **bidirectional synchronization** between Spotify and Tidal. Due to various performance optimizations, it is particularly suited for periodic synchronization of very large collections.

Supports:
- **Spotify → Tidal**: Import your Spotify playlists into Tidal
- **Tidal → Spotify**: Import your Tidal playlists into Spotify

## Installation

```bash
git clone https://github.com/steff-sson/music-sync.git
cd music-sync
python3 -m pip install -e .
```

For a new virtual environment:

```bash
git clone https://github.com/steff-sson/music-sync.git
cd music-sync
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

With dev dependencies (for running tests):

```bash
source .venv/bin/activate
pip install -e ".[dev]"
```

**Note:** The `--clean` feature requires `musicbrainzngs`, which is included in the default dependencies.

## Setup

1. Copy `config.yml.example` to `config.yml`
2. Go to [Spotify Developer Dashboard](https://developer.spotify.com/documentation/general/guides/authorization/app-settings/) and register a new app
3. Add Redirect URI: `http://127.0.0.1:8888/callback` in your Spotify app settings
4. Copy your Spotify `client_id` and `client_secret` into `config.yml`
5. Add your Spotify `username`
6. On first run, Tidal will open a browser for OAuth login

## Usage

### Sync Direction

Specify direction as positional arguments: `INPUT OUTPUT`

### Tidal → Spotify

```bash
music-sync tidal spotify                    # Sync all Tidal playlists to Spotify
music-sync tidal spotify --uri <playlist_id>  # Sync specific playlist
music-sync tidal spotify --dry-run           # Preview what would be synced
music-sync tidal spotify --sync-favorites   # Sync liked songs
```

### Spotify → Tidal

```bash
music-sync spotify tidal                    # Sync all Spotify playlists to Tidal
music-sync spotify tidal --uri <playlist_uri> # Sync specific playlist
music-sync spotify tidal --dry-run           # Preview what would be synced
```

### Clean - Organize into Genre Playlists

**Requires:** `musicbrainzngs` (installed by default with `pip install -e .`)

Organize your favorites or any playlist into genre-based playlists **on the same platform**.

```bash
# Spotify favorites → Spotify genre playlists
music-sync spotify --clean                    # Organize favorites into genre playlists
music-sync spotify --clean --dry-run         # Preview without making changes
music-sync spotify --clean --uri <playlist>  # Clean specific playlist

# Tidal favorites → Tidal genre playlists
music-sync tidal --clean                    # Organize favorites into genre playlists
music-sync tidal --clean --dry-run          # Preview without making changes
music-sync tidal --clean --uri <playlist>   # Clean specific playlist
```

Genre playlists are named as `{original_playlist_name}-{GENRE}` (e.g., `My Favorites-ROCK`, `TestPlaylist-DANCE`).

After creating genre playlists, **manually delete** the source in Spotify/Tidal:
- Spotify: Bibliothek → Lieblingssongs → alle auswählen → Entfernen
- Tidal: Playlist öffnen → ⋮ → Von Bibliothek entfernen

### Dry Run

Use `--dry-run` to preview changes before applying them. This shows:
- Which tracks would be added to new/existing playlists
- Which tracks already exist (by ISRC matching)
- Which tracks couldn't be matched on the target platform
- Summary report of all pending changes

## Configuration

See `config.yml.example` for all available options:

| Option | Description | Default |
|--------|-------------|---------|
| `max_concurrency` | Max parallel API requests | 10 |
| `rate_limit` | Max requests per second | 10 |
| `sync_favorites_default` | Auto-sync favorites | true |
| `excluded_playlists` | Skip certain playlists | [] |

## Not Found Tracks

When tracks cannot be matched on the target platform, they are logged to:
- `songs_not_found.txt` (Spotify → Tidal)
- `songs_not_found_tidal_to_spotify.txt` (Tidal → Spotify)

## License

AGPL v3 - See [LICENSE](LICENSE)

---

#### Join our amazing community as a code contributor
<br><br>
<a href="https://github.com/steff-sson/music-sync/graphs/contributors">
  <img class="dark-light" src="https://contrib.rocks/image?repo=steff-sson/music-sync&anon=0&columns=25&max=100&r=true" />
</a>