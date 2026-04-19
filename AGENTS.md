# Agent Instructions for spotify_to_tidal

This file provides guidance for agentic coding agents working in this repository.

## Project Overview

A Python CLI tool for importing Spotify playlists into Tidal. Uses async/await patterns for concurrent API operations.

## Build, Lint, and Test Commands

### Installation (Development)
```bash
pip install -e .          # Install package in editable mode
pip install -e ".[dev]"    # Install with dev dependencies (if defined)
```

### Running Tests
```bash
pytest                        # Run all tests (maxfail=1, warnings disabled by default)
pytest tests/                 # Run all tests in tests directory
pytest tests/unit/            # Run unit tests only
pytest tests/unit/test_auth.py              # Run single test file
pytest tests/unit/test_auth.py::test_open_spotify_session  # Run specific test
pytest -v                    # Run with verbose output
pytest -k "test_name"        # Run tests matching pattern
```

### Package Management
```bash
pip install -r Pipfile       # Install from Pipfile.lock
pip freeze > requirements.txt # Export dependencies (if needed)
```

### Running the Application
```bash
spotify_to_tidal                              # Run with config.yml in working directory
python -m spotify_to_tidal                   # Alternative invocation
spotify_to_tidal --config path/to/config.yml # Custom config location
spotify_to_tidal --uri <playlist_uri>         # Sync specific playlist
spotify_to_tidal --sync-favorites            # Sync liked songs only
```

## Code Style Guidelines

### General
- Python 3.10+ required
- Use `#!/usr/bin/env python3` shebang for executable scripts
- Follow existing patterns in the codebase

### Type Annotations
- Use `TypedDict` for structured dict types (see `src/spotify_to_tidal/type/`)
- Use `typing` module imports: `TypedDict`, `List`, `Dict`, `Mapping`, `Sequence`, `Set`, `Optional`, `Callable`
- Union types: prefer `X | Y` syntax over `Union[X, Y]` for Python 3.10+
- Use `X | None` syntax (not `Optional[X]`)

### Imports
- Standard library first, then third-party, then local
- Use absolute imports within package: `from spotify_to_tidal import sync`
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
src/spotify_to_tidal/
  __main__.py      # CLI entry point
  auth.py          # Authentication (Spotify/Tidal sessions)
  sync.py          # Core sync logic (async)
  cache.py         # SQLAlchemy cache for match failures
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
- Both support rate limiting - respect `max_concurrency` config
- Cache failed lookups to avoid repeated failures
