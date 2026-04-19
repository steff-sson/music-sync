# Agent Instructions for music-sync

This file provides guidance for agentic coding agents working in this repository.

## Project Overview

A Python CLI tool for bidirectional synchronization between Spotify and Tidal. Uses async/await patterns for concurrent API operations.

Supports Spotify → Tidal and Tidal → Spotify sync.

## Build, Lint, and Test Commands

### IMPORTANT: Python Environment Setup
**NEVER use `pip install` with `--break-system-packages` or `--user` flags.**
**NEVER install packages globally outside of a virtual environment.**
This can break the system Python on Arch Linux and brick pacman.

**Always use the project virtual environment:**
```bash
source .venv/bin/activate      # Activate venv (do this FIRST)
pip install -e .                # Install package in editable mode
pip install -e ".[dev]"         # Install with dev dependencies
```

### Installation (Development)
```bash
# First time setup
python -m venv .venv           # Create virtual environment
source .venv/bin/activate      # Activate venv
pip install -e .               # Install package in editable mode
pip install -e ".[dev]"        # Install with dev dependencies (if defined)
```

### Running Tests
```bash
source .venv/bin/activate && pytest  # Run all tests (maxfail=1, warnings disabled by default)
source .venv/bin/activate && pytest tests/                 # Run all tests in tests directory
source .venv/bin/activate && pytest tests/unit/            # Run unit tests only
source .venv/bin/activate && pytest tests/unit/test_auth.py              # Run single test file
source .venv/bin/activate && pytest tests/unit/test_auth.py::test_open_spotify_session  # Run specific test
source .venv/bin/activate && pytest -v                    # Run with verbose output
source .venv/bin/activate && pytest -k "test_name"        # Run tests matching pattern
```

### Running the Application
```bash
source .venv/bin/activate                              # Activate venv FIRST
music-sync                              # Run with config.yml (Spotify → Tidal, default)
music-sync tidal spotify                # Tidal → Spotify sync
music-sync spotify tidal                # Spotify → Tidal sync
music-sync tidal spotify --dry-run      # Preview Tidal → Spotify sync
music-sync tidal spotify --dry-run      # Preview Spotify → Tidal sync
music-sync --config path/to/config.yml # Custom config location
music-sync --uri <playlist_uri>         # Sync specific playlist
music-sync --sync-favorites            # Sync liked songs
music-sync --tidal-to-spotify --dry-run # Legacy flag (still works)
```

## Code Style Guidelines

### General
- Python 3.10+ required
- Use `#!/usr/bin/env python3` shebang for executable scripts
- Follow existing patterns in the codebase

### Type Annotations
- Use `TypedDict` for structured dict types (see `src/music_sync/type/`)
- Use `typing` module imports: `TypedDict`, `List`, `Dict`, `Mapping`, `Sequence`, `Set`, `Optional`, `Callable`
- Union types: prefer `X | Y` syntax over `Union[X, Y]` for Python 3.10+
- Use `X | None` syntax (not `Optional[X]`)

### Imports
- Standard library first, then third-party, then local
- Use absolute imports within package: `from music_sync import sync`
- Relative imports for internal modules: `from . import auth`
- Group by type with blank lines between groups

### Naming Conventions
- Classes: `CapWords` (e.g., `SpotifyTrack`, `MatchFailureDatabase`)
- Functions/variables: `snake_case` (e.g., `open_spotify_session`, `track_match_cache`)
- Constants: `UPPER_SNAKE_CASE` (e.g., `SPOTIFY_SCOPES`)
- Type aliases: `X = Y` style (e.g., `SpotifyID = str`)
- Private members: prefix with `_` (e.g., `_fetch_all_from_spotify_in_chunks`)

### Function Design
- Async functions for I/O operations (API calls, file I/O)
- Use `asyncio.to_thread` to wrap blocking sync functions
- Retry patterns with exponential backoff for unreliable operations
- Use `functools.partial` for passing extra arguments to callbacks

### Error Handling
- Use specific exception types when possible
- Retry transient errors (rate limiting, network issues)
- Call `sys.exit(1)` for unrecoverable failures with error context
- Print descriptive error messages before exiting

### Async Patterns
```python
async def example(..., semaphore, ...):
    await rate_limiter.acquire()
    result = await asyncio.to_thread(blocking_function, args)
    # ... use result

# Rate limiting with semaphore
semaphore = asyncio.Semaphore(config.get('max_concurrency', 10))
rate_limiter_task = asyncio.create_task(_run_rate_limiter(semaphore))
results = await atqdm.gather(*[async_task for item in items], desc="...")
rate_limiter_task.cancel()
```

### Testing
- Use `pytest` with `pytest-mock` for mocking
- Place tests in `tests/unit/` directory
- Test file naming: `test_<module_name>.py`
- Test class naming: `Test*`
- Test function naming: `test_*`
- Use `mocker.patch` with `autospec=True` for mocks

### Module Structure
```
src/music_sync/
  __main__.py      # CLI entry point
  auth.py          # Authentication (Spotify/Tidal sessions)
  sync.py          # Core sync logic (async)
  sync_engine.py   # Cache-based bidirectional sync engine (new)
  matcher.py       # Bidirectional matching logic (new)
  cache.py         # SQLAlchemy cache for match failures
  cache_db.py      # UnifiedTrackCache - SQLite cache for tracks (new)
  tidalapi_patch.py # Patches/wrappers for tidalapi
  type/
    spotify.py     # Spotify TypedDict definitions
    config.py      # Config TypedDict definitions
```

### File Organization
- Sort imports at top of file
- Helper functions defined near their usage
- Keep functions focused and small
- Use descriptive variable names

### Working with External APIs
- Spotify: uses `spotipy` library
- Tidal: uses `tidalapi` library
- Both support rate limiting - respect `max_concurrency` config (default: 3)
- Cache failed lookups to avoid repeated failures
- **Rate limiting:** Conservative settings (rate_limit=3, max_concurrency=3) to avoid 429 errors
- **Exponential backoff:** When 429 hit, cooldown starts at 30s and doubles (max 300s)

### Cache and Log Files
- `.tracks.db` - SQLite database with UnifiedTrackCache (not tracked in git)
- `songs_not_found_tidal_to_spotify.txt` - Tidal tracks not found on Spotify
- `songs_not_found_spotify_to_tidal.txt` - Spotify tracks not found on Tidal
- `.cache.db` - Match failure cache (tracks that couldn't be matched)
- These files are gitignored

### Current Status (2026-04-19)
- Tidal → Spotify sync implemented
- Spotify → Tidal sync implemented
- UnifiedTrackCache with SQLite backend
- BidirectionalMatcher for ISRC + fuzzy matching
- Not-found tracking with user-readable log files
- Rate limit protection with exponential backoff
- **PAUSED:** Spotify rate limit hit (78449s). Will resume tomorrow.