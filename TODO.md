# music-sync Todo

## Status (2026-04-19)

### ✅ Abgeschlossen
- [x] Tidal → Spotify Sync implementiert
- [x] Spotify → Tidal Sync implementiert
- [x] UnifiedTrackCache (SQLite) für bidirektionales Caching
- [x] BidirectionalMatcher (ISRC + fuzzy matching)
- [x] Not-found Log-Files (`songs_not_found_tidal_to_spotify.txt`, etc.)
- [x] Rate Limit Protection (rate_limit=3, max_concurrency=3)
- [x] Exponential Backoff bei 429 Errors
- [x] Cache-Integration in sync.py
- [x] CLI erweitert: `music-sync tidal spotify`
- [x] AGENTS.md aktualisiert

### ⏸️ PAUSED
- [ ] **Spotify Rate Limit** - 78449s (~21h) bis Reset
- [ ] Dry-run Test (nach Limit-Reset)
- [ ] Echter Sync Tidal → Spotify

---

## Nächste Schritte (nach Rate Limit Reset)

### Sofort
1. Spotify Rate Limit Status prüfen
2. Dry-run starten: `music-sync tidal spotify --dry-run`
3. Ergebnisse checken: `songs_not_found_tidal_to_spotify.txt`

### Bei Problemen
- [ ] rate_limit weiter senken (von 3 auf 2) wenn wieder 429s
- [ ] Neue Spotify App erstellen falls nötig

### Optionale Optimierungen
- [ ] Success Cache implementieren (Tidal ID → Spotify ID Mappings)
- [ ] Batch-Insert in sync_engine nutzen
- [ ] `--force-refresh` Flag für Cache-Erneuerung

---

## Bekannte Issues

1. **Spotify Rate Limit** - Spotify Dev Account Limitation
   - Workaround: Konservative Rate Limits + Exponential Backoff
   - Alternative: Neue Spotify App erstellen

2. **Performance** - ~3 searches/sec bei rate_limit=3
   - Erwartete Zeit für 6000 Tracks: ~30-40 min
   - Mit Cache: Folgende Syncs deutlich schneller

---

## Dateien

| Datei | Beschreibung |
|-------|--------------|
| `.tracks.db` | SQLite Cache (Tidal/Spotify Tracks) |
| `.cache.db` | Match Failure Cache |
| `songs_not_found_tidal_to_spotify.txt` | Nicht-gefundene Tidal Tracks |
| `songs_not_found_spotify_to_tidal.txt` | Nicht-gefundene Spotify Tracks |
| `config.yml` | Credentials und Settings |
| `.session.yml` | Tidal Session Token |
