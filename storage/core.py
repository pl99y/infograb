from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent

SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS sources (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    source_type TEXT NOT NULL,
    url TEXT NOT NULL UNIQUE,
    enabled INTEGER NOT NULL DEFAULT 1,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id INTEGER NOT NULL,
    post_url TEXT NOT NULL UNIQUE,
    content_text_raw TEXT,
    content_text_clean TEXT,
    content_text_zh TEXT,
    content_text_en TEXT,
    published_at TEXT,
    fetched_at TEXT NOT NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (source_id) REFERENCES sources(id)
);

CREATE TABLE IF NOT EXISTS item_media (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    item_id INTEGER NOT NULL,
    media_type TEXT NOT NULL,
    media_url TEXT NOT NULL,
    local_path TEXT,
    download_status TEXT NOT NULL DEFAULT 'pending',
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (item_id) REFERENCES items(id),
    UNIQUE(item_id, media_url)
);

CREATE TABLE IF NOT EXISTS fetch_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id INTEGER,
    source_name TEXT,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    status TEXT NOT NULL,
    items_found INTEGER DEFAULT 0,
    new_items INTEGER DEFAULT 0,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS energy_quotes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    quote_key TEXT NOT NULL,
    name TEXT NOT NULL,
    price REAL,
    unit TEXT,
    change REAL,
    change_percent REAL,
    timestamp_text TEXT,
    source_name TEXT NOT NULL,
    source_url TEXT NOT NULL,
    provider_used TEXT NOT NULL,
    is_delayed INTEGER,
    delay_note TEXT,
    extra_json TEXT,
    fetched_at TEXT NOT NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);



CREATE TABLE IF NOT EXISTS market_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_key TEXT NOT NULL,
    name TEXT NOT NULL,
    symbol TEXT NOT NULL,
    price REAL,
    change_percent REAL,
    source_name TEXT NOT NULL,
    fetched_at TEXT NOT NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS iflow_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_key TEXT NOT NULL,
    today_factor REAL,
    database_size INTEGER,
    page_updated_at TEXT,
    rank INTEGER,
    item_name TEXT NOT NULL,
    daily_volume TEXT,
    min_price TEXT,
    best_sell_ratio TEXT,
    best_buy_ratio TEXT,
    safe_buy_ratio TEXT,
    recent_ratio TEXT,
    platform TEXT,
    steam_market TEXT,
    updated_text TEXT,
    source_name TEXT NOT NULL,
    source_url TEXT NOT NULL,
    fetched_at TEXT NOT NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS airport_alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dedupe_key TEXT NOT NULL UNIQUE,
    callsign TEXT NOT NULL,
    status_text TEXT,
    alert_type TEXT,
    squawk_code TEXT,
    event_date_text TEXT,
    departure_time_text TEXT,
    departure_airport TEXT,
    arrival_time_text TEXT,
    arrival_airport TEXT,
    duration_text TEXT,
    aircraft_text TEXT,
    distance_text TEXT,
    age_hours REAL,
    source_name TEXT NOT NULL,
    source_url TEXT NOT NULL,
    extra_json TEXT,
    fetched_at TEXT NOT NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS airport_disruptions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    region TEXT NOT NULL,
    period TEXT NOT NULL,
    direction TEXT NOT NULL,
    rank INTEGER,
    airport_name TEXT NOT NULL,
    iata TEXT NOT NULL,
    country TEXT,
    disruption_index REAL,
    canceled_flights INTEGER,
    canceled_percent REAL,
    delayed_flights INTEGER,
    delayed_percent REAL,
    average_delay_min INTEGER,
    source_name TEXT NOT NULL,
    source_url TEXT NOT NULL,
    extra_json TEXT,
    fetched_at TEXT NOT NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS weather_alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dedupe_key TEXT NOT NULL UNIQUE,
    source_primary TEXT NOT NULL,
    title TEXT NOT NULL,
    summary TEXT,
    severity_level TEXT,
    color_level TEXT,
    event_type TEXT,
    location_text TEXT,
    issued_at TEXT,
    effective_at TEXT,
    expires_at TEXT,
    source_url TEXT,
    detail_url TEXT,
    payload_json TEXT,
    fetched_at TEXT NOT NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS disaster_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_family TEXT NOT NULL DEFAULT 'instant',
    event_type TEXT NOT NULL,
    severity_level TEXT,
    severity_color TEXT,
    title TEXT,
    summary TEXT,
    occurred_at TEXT,
    updated_at TEXT,
    fetched_at TEXT,
    location_text TEXT,
    lat REAL,
    lon REAL,
    source_primary TEXT NOT NULL,
    source_secondary TEXT,
    external_id TEXT,
    external_id_secondary TEXT,
    dedupe_key TEXT NOT NULL UNIQUE,
    status TEXT,
    map_url TEXT,
    payload_json TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS disaster_ongoing_groups (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    group_key TEXT NOT NULL UNIQUE,
    event_type TEXT NOT NULL,
    source_primary TEXT NOT NULL,
    source_secondary TEXT,
    title TEXT NOT NULL,
    summary TEXT,
    severity_level TEXT,
    severity_color TEXT,
    status TEXT,
    started_at TEXT,
    updated_at TEXT,
    location_text TEXT,
    lat REAL,
    lon REAL,
    country_code TEXT,
    map_url TEXT,
    official_link TEXT,
    payload_json TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS disaster_ongoing_members (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    group_id INTEGER NOT NULL,
    event_id INTEGER NOT NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (group_id) REFERENCES disaster_ongoing_groups(id) ON DELETE CASCADE,
    FOREIGN KEY (event_id) REFERENCES disaster_events(id) ON DELETE CASCADE,
    UNIQUE(group_id, event_id)
);

CREATE TABLE IF NOT EXISTS swpc_alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dedupe_key TEXT NOT NULL UNIQUE,
    product_id TEXT,
    message_code TEXT,
    serial_number TEXT,
    issue_datetime TEXT NOT NULL,
    issue_time_text TEXT,
    headline TEXT NOT NULL,
    message_type TEXT,
    noaa_scale TEXT,
    details_json TEXT,
    impacts_text TEXT,
    description_text TEXT,
    message_raw TEXT NOT NULL,
    source_name TEXT NOT NULL,
    source_url TEXT NOT NULL,
    fetched_at TEXT NOT NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS swpc_forecasts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    forecast_key TEXT NOT NULL,
    forecast_issued_at TEXT,
    geomag_issued_at TEXT,
    panel_json TEXT NOT NULL,
    raw_forecast_text TEXT NOT NULL,
    raw_geomag_text TEXT NOT NULL,
    source_name TEXT NOT NULL,
    source_url TEXT NOT NULL,
    fetched_at TEXT NOT NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS space_launch_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_key TEXT NOT NULL,
    items_json TEXT NOT NULL,
    source_name TEXT NOT NULL,
    source_url TEXT NOT NULL,
    fetched_at TEXT NOT NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS public_health_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dedupe_key TEXT NOT NULL UNIQUE,
    source_key TEXT NOT NULL,
    source_name TEXT NOT NULL,
    category_key TEXT NOT NULL,
    title_raw TEXT NOT NULL,
    title_zh TEXT,
    date_raw TEXT,
    published_at TEXT,
    item_url TEXT,
    list_url TEXT NOT NULL,
    rank INTEGER,
    translation_provider TEXT,
    payload_json TEXT,
    fetched_at TEXT NOT NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS news_timeline_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dedupe_key TEXT NOT NULL UNIQUE,
    source_id INTEGER,
    source_key TEXT NOT NULL,
    source_name TEXT NOT NULL,
    channel TEXT,
    topic TEXT,
    title TEXT NOT NULL,
    url TEXT NOT NULL,
    published_at TEXT,
    published_text TEXT,
    author_names_json TEXT,
    list_url TEXT,
    payload_json TEXT,
    fetched_at TEXT NOT NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (source_id) REFERENCES sources(id)
);

CREATE INDEX IF NOT EXISTS idx_news_timeline_published_at ON news_timeline_items(published_at);
CREATE INDEX IF NOT EXISTS idx_news_timeline_fetched_at ON news_timeline_items(fetched_at);
CREATE INDEX IF NOT EXISTS idx_news_timeline_source_key ON news_timeline_items(source_key);

CREATE TABLE IF NOT EXISTS mnd_pla_daily (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    report_date TEXT NOT NULL UNIQUE,
    published_date_raw TEXT,
    period_start TEXT,
    period_end TEXT,
    report_period_raw TEXT,
    title TEXT NOT NULL,
    post_url TEXT NOT NULL UNIQUE,
    body TEXT NOT NULL,
    activity_text TEXT,
    no_aircraft INTEGER NOT NULL DEFAULT 0,
    aircraft_total INTEGER,
    aircraft_intrusion_total INTEGER,
    ship_total INTEGER,
    official_ship_total INTEGER,
    balloon_total INTEGER,
    intrusion_areas_json TEXT NOT NULL,
    source_name TEXT NOT NULL,
    source_url TEXT NOT NULL,
    fetched_at TEXT NOT NULL,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_items_fetched_at ON items(fetched_at);
CREATE INDEX IF NOT EXISTS idx_item_media_item_id ON item_media(item_id);
CREATE INDEX IF NOT EXISTS idx_energy_quotes_fetched_at ON energy_quotes(fetched_at);
CREATE INDEX IF NOT EXISTS idx_market_snapshots_fetched_at ON market_snapshots(fetched_at);
CREATE INDEX IF NOT EXISTS idx_iflow_snapshots_fetched_at ON iflow_snapshots(fetched_at);
CREATE INDEX IF NOT EXISTS idx_airport_alerts_fetched_at ON airport_alerts(fetched_at);
CREATE INDEX IF NOT EXISTS idx_airport_disruptions_fetched_at ON airport_disruptions(fetched_at);
CREATE INDEX IF NOT EXISTS idx_fetch_runs_started_at ON fetch_runs(started_at);
CREATE INDEX IF NOT EXISTS idx_weather_alerts_fetched_at ON weather_alerts(fetched_at);
CREATE INDEX IF NOT EXISTS idx_disaster_events_updated_at ON disaster_events(updated_at);
CREATE INDEX IF NOT EXISTS idx_disaster_ongoing_updated_at ON disaster_ongoing_groups(updated_at);
CREATE INDEX IF NOT EXISTS idx_disaster_ongoing_members_group_id ON disaster_ongoing_members(group_id);
CREATE INDEX IF NOT EXISTS idx_swpc_alerts_issue_datetime ON swpc_alerts(issue_datetime);
CREATE INDEX IF NOT EXISTS idx_swpc_alerts_fetched_at ON swpc_alerts(fetched_at);
CREATE INDEX IF NOT EXISTS idx_swpc_forecasts_fetched_at ON swpc_forecasts(fetched_at);
CREATE INDEX IF NOT EXISTS idx_space_launch_snapshots_fetched_at ON space_launch_snapshots(fetched_at);
CREATE INDEX IF NOT EXISTS idx_mnd_pla_daily_report_date ON mnd_pla_daily(report_date);
CREATE INDEX IF NOT EXISTS idx_mnd_pla_daily_fetched_at ON mnd_pla_daily(fetched_at);
"""

_MIGRATION_SQL = """
ALTER TABLE items ADD COLUMN content_text_zh TEXT;
ALTER TABLE items ADD COLUMN content_text_en TEXT;
ALTER TABLE disaster_events ADD COLUMN event_family TEXT DEFAULT 'instant';
ALTER TABLE disaster_events ADD COLUMN fetched_at TEXT;
ALTER TABLE disaster_events ADD COLUMN severity_color TEXT;
ALTER TABLE disaster_events ADD COLUMN map_url TEXT;
ALTER TABLE disaster_events ADD COLUMN external_id_secondary TEXT;
"""


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def resolve_db_path(db_path: str | Path = "app.db") -> Path:
    path = Path(db_path)
    if path.is_absolute():
        return path
    return (PROJECT_ROOT / path).resolve()


def get_connection(db_path: str | Path = "app.db") -> sqlite3.Connection:
    resolved = resolve_db_path(db_path)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(resolved))
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys = ON")
    return con


def initialize_database(db_path: str | Path = "app.db") -> None:
    con = get_connection(db_path)
    try:
        con.executescript(SCHEMA_SQL)
        for stmt in _MIGRATION_SQL.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                try:
                    con.execute(stmt)
                except Exception:
                    pass
        con.commit()
    finally:
        con.close()


def _table_exists(con: sqlite3.Connection, table_name: str) -> bool:
    row = con.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def _json_dumps(value) -> str:
    return json.dumps(value or {}, ensure_ascii=False)


def upsert_source(
    con: sqlite3.Connection,
    name: str,
    source_type: str,
    url: str,
    enabled: bool = True,
) -> int:
    con.execute(
        """
        INSERT INTO sources (name, source_type, url, enabled)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(url) DO UPDATE SET
            name = excluded.name,
            source_type = excluded.source_type,
            enabled = excluded.enabled
        """,
        (name, source_type, url, int(enabled)),
    )
    row = con.execute("SELECT id FROM sources WHERE url = ?", (url,)).fetchone()
    return int(row["id"])


def insert_item_if_new(
    con: sqlite3.Connection,
    source_id: int,
    post_url: str,
    content_text_raw: str,
    content_text_clean: str,
    published_at: str,
    fetched_at: str,
) -> tuple[int, bool]:
    existing = con.execute("SELECT id FROM items WHERE post_url = ?", (post_url,)).fetchone()
    if existing:
        return int(existing["id"]), False

    cur = con.execute(
        """
        INSERT INTO items (
            source_id,
            post_url,
            content_text_raw,
            content_text_clean,
            published_at,
            fetched_at
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            source_id,
            post_url,
            content_text_raw,
            content_text_clean,
            published_at,
            fetched_at,
        ),
    )
    return int(cur.lastrowid), True


def insert_media_if_new(
    con: sqlite3.Connection,
    item_id: int,
    media_type: str,
    media_url: str,
) -> tuple[int, bool]:
    existing = con.execute(
        "SELECT id FROM item_media WHERE item_id = ? AND media_url = ?",
        (item_id, media_url),
    ).fetchone()
    if existing:
        return int(existing["id"]), False

    cur = con.execute(
        """
        INSERT INTO item_media (item_id, media_type, media_url, local_path, download_status)
        VALUES (?, ?, ?, NULL, 'pending')
        """,
        (item_id, media_type, media_url),
    )
    return int(cur.lastrowid), True


def update_media_download(
    con: sqlite3.Connection,
    media_id: int,
    local_path: str | None,
    download_status: str,
) -> None:
    con.execute(
        "UPDATE item_media SET local_path = ?, download_status = ? WHERE id = ?",
        (local_path, download_status, media_id),
    )


def insert_energy_quote(
    con: sqlite3.Connection,
    *,
    quote_key: str,
    name: str,
    price: float | None,
    unit: str | None,
    change: float | None,
    change_percent: float | None,
    timestamp_text: str | None,
    source_name: str,
    source_url: str,
    provider_used: str,
    is_delayed: bool | None,
    delay_note: str | None,
    extra: dict | None,
    fetched_at: str,
) -> int:
    cur = con.execute(
        """
        INSERT INTO energy_quotes (
            quote_key, name, price, unit, change, change_percent, timestamp_text,
            source_name, source_url, provider_used, is_delayed, delay_note, extra_json, fetched_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            quote_key,
            name,
            price,
            unit,
            change,
            change_percent,
            timestamp_text,
            source_name,
            source_url,
            provider_used,
            None if is_delayed is None else int(is_delayed),
            delay_note,
            _json_dumps(extra),
            fetched_at,
        ),
    )
    return int(cur.lastrowid)




def insert_market_snapshot(
    con: sqlite3.Connection,
    *,
    snapshot_key: str,
    name: str,
    symbol: str,
    price: float | None,
    change_percent: float | None,
    source_name: str,
    fetched_at: str,
) -> int:
    cur = con.execute(
        """
        INSERT INTO market_snapshots (
            snapshot_key, name, symbol, price, change_percent, source_name, fetched_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            snapshot_key,
            name,
            symbol,
            price,
            change_percent,
            source_name,
            fetched_at,
        ),
    )
    return int(cur.lastrowid)


def insert_iflow_snapshot_row(
    con: sqlite3.Connection,
    *,
    snapshot_key: str,
    today_factor: float | None,
    database_size: int | None,
    page_updated_at: str | None,
    rank: int | None,
    item_name: str,
    daily_volume: str | None,
    min_price: str | None,
    best_sell_ratio: str | None,
    best_buy_ratio: str | None,
    safe_buy_ratio: str | None,
    recent_ratio: str | None,
    platform: str | None,
    steam_market: str | None,
    updated_text: str | None,
    source_name: str,
    source_url: str,
    fetched_at: str,
) -> int:
    cur = con.execute(
        """
        INSERT INTO iflow_snapshots (
            snapshot_key, today_factor, database_size, page_updated_at, rank, item_name,
            daily_volume, min_price, best_sell_ratio, best_buy_ratio, safe_buy_ratio,
            recent_ratio, platform, steam_market, updated_text, source_name, source_url, fetched_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            snapshot_key,
            today_factor,
            database_size,
            page_updated_at,
            rank,
            item_name,
            daily_volume,
            min_price,
            best_sell_ratio,
            best_buy_ratio,
            safe_buy_ratio,
            recent_ratio,
            platform,
            steam_market,
            updated_text,
            source_name,
            source_url,
            fetched_at,
        ),
    )
    return int(cur.lastrowid)


def insert_airport_alert(
    con: sqlite3.Connection,
    *,
    dedupe_key: str,
    callsign: str,
    status_text: str | None,
    alert_type: str | None,
    squawk_code: str | None,
    event_date_text: str | None,
    departure_time_text: str | None,
    departure_airport: str | None,
    arrival_time_text: str | None,
    arrival_airport: str | None,
    duration_text: str | None,
    aircraft_text: str | None,
    distance_text: str | None,
    age_hours: float | None,
    source_name: str,
    source_url: str,
    extra: dict | None,
    fetched_at: str,
) -> tuple[int | None, bool]:
    existing = con.execute(
        "SELECT id FROM airport_alerts WHERE dedupe_key = ?",
        (dedupe_key,),
    ).fetchone()
    if existing:
        return int(existing["id"]), False

    cur = con.execute(
        """
        INSERT INTO airport_alerts (
            dedupe_key, callsign, status_text, alert_type, squawk_code, event_date_text,
            departure_time_text, departure_airport, arrival_time_text, arrival_airport,
            duration_text, aircraft_text, distance_text, age_hours, source_name, source_url,
            extra_json, fetched_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            dedupe_key,
            callsign,
            status_text,
            alert_type,
            squawk_code,
            event_date_text,
            departure_time_text,
            departure_airport,
            arrival_time_text,
            arrival_airport,
            duration_text,
            aircraft_text,
            distance_text,
            age_hours,
            source_name,
            source_url,
            _json_dumps(extra),
            fetched_at,
        ),
    )
    return int(cur.lastrowid), True


def insert_airport_disruption(
    con: sqlite3.Connection,
    *,
    region: str,
    period: str,
    direction: str,
    rank: int | None,
    airport_name: str,
    iata: str,
    country: str | None,
    disruption_index: float | None,
    canceled_flights: int | None,
    canceled_percent: float | None,
    delayed_flights: int | None,
    delayed_percent: float | None,
    average_delay_min: int | None,
    source_name: str,
    source_url: str,
    extra: dict | None,
    fetched_at: str,
) -> int:
    cur = con.execute(
        """
        INSERT INTO airport_disruptions (
            region, period, direction, rank, airport_name, iata, country, disruption_index,
            canceled_flights, canceled_percent, delayed_flights, delayed_percent,
            average_delay_min, source_name, source_url, extra_json, fetched_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            region,
            period,
            direction,
            rank,
            airport_name,
            iata,
            country,
            disruption_index,
            canceled_flights,
            canceled_percent,
            delayed_flights,
            delayed_percent,
            average_delay_min,
            source_name,
            source_url,
            _json_dumps(extra),
            fetched_at,
        ),
    )
    return int(cur.lastrowid)


def upsert_weather_alert(
    con: sqlite3.Connection,
    *,
    dedupe_key: str,
    source_primary: str,
    title: str,
    summary: str | None,
    severity_level: str | None,
    color_level: str | None,
    event_type: str | None,
    location_text: str | None,
    issued_at: str | None,
    effective_at: str | None,
    expires_at: str | None,
    source_url: str | None,
    detail_url: str | None,
    payload: dict | None,
    fetched_at: str | None = None,
) -> tuple[int, bool]:
    fetched_at = fetched_at or issued_at or _utc_now_iso()
    existing = con.execute(
        "SELECT id FROM weather_alerts WHERE dedupe_key = ?",
        (dedupe_key,),
    ).fetchone()

    if existing:
        con.execute(
            """
            UPDATE weather_alerts
            SET source_primary = ?, title = ?, summary = ?, severity_level = ?, color_level = ?,
                event_type = ?, location_text = ?, issued_at = ?, effective_at = ?, expires_at = ?,
                source_url = ?, detail_url = ?, payload_json = ?, fetched_at = ?
            WHERE id = ?
            """,
            (
                source_primary,
                title,
                summary,
                severity_level,
                color_level,
                event_type,
                location_text,
                issued_at,
                effective_at,
                expires_at,
                source_url,
                detail_url,
                _json_dumps(payload),
                fetched_at,
                int(existing["id"]),
            ),
        )
        return int(existing["id"]), False

    cur = con.execute(
        """
        INSERT INTO weather_alerts (
            dedupe_key, source_primary, title, summary, severity_level, color_level, event_type,
            location_text, issued_at, effective_at, expires_at, source_url, detail_url, payload_json, fetched_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            dedupe_key,
            source_primary,
            title,
            summary,
            severity_level,
            color_level,
            event_type,
            location_text,
            issued_at,
            effective_at,
            expires_at,
            source_url,
            detail_url,
            _json_dumps(payload),
            fetched_at,
        ),
    )
    return int(cur.lastrowid), True


def upsert_disaster_event(
    con: sqlite3.Connection,
    *,
    event_family: str | None = None,
    event_type: str,
    severity_level: str | None,
    severity_color: str | None,
    title: str | None,
    summary: str | None,
    occurred_at: str | None,
    updated_at: str | None,
    location_text: str | None,
    lat: float | None,
    lon: float | None,
    source_primary: str,
    source_secondary: str | None,
    external_id: str | None,
    external_id_secondary: str | None,
    dedupe_key: str,
    status: str | None,
    map_url: str | None,
    payload: dict | None,
    fetched_at: str | None = None,
) -> tuple[int, bool]:
    fetched_at = fetched_at or updated_at or occurred_at or _utc_now_iso()
    event_family = event_family or "instant"
    existing = con.execute(
        "SELECT id FROM disaster_events WHERE dedupe_key = ?",
        (dedupe_key,),
    ).fetchone()
    columns = _table_columns(con, "disaster_events")
    has_event_family = "event_family" in columns

    update_fields: list[tuple[str, object]] = []
    if has_event_family:
        update_fields.append(("event_family", event_family))
    update_fields.extend([
        ("event_type", event_type),
        ("severity_level", severity_level),
        ("severity_color", severity_color),
        ("title", title),
        ("summary", summary),
        ("occurred_at", occurred_at),
        ("updated_at", updated_at),
        ("fetched_at", fetched_at),
        ("location_text", location_text),
        ("lat", lat),
        ("lon", lon),
        ("source_primary", source_primary),
        ("source_secondary", source_secondary),
        ("external_id", external_id),
        ("external_id_secondary", external_id_secondary),
        ("status", status),
        ("map_url", map_url),
        ("payload_json", _json_dumps(payload)),
    ])

    if existing:
        assignments = ", ".join(f"{name} = ?" for name, _ in update_fields)
        values = [value for _, value in update_fields] + [int(existing["id"])]
        con.execute(
            f"UPDATE disaster_events SET {assignments} WHERE id = ?",
            values,
        )
        return int(existing["id"]), False

    insert_fields: list[tuple[str, object]] = []
    if has_event_family:
        insert_fields.append(("event_family", event_family))
    insert_fields.extend([
        ("event_type", event_type),
        ("severity_level", severity_level),
        ("severity_color", severity_color),
        ("title", title),
        ("summary", summary),
        ("occurred_at", occurred_at),
        ("updated_at", updated_at),
        ("fetched_at", fetched_at),
        ("location_text", location_text),
        ("lat", lat),
        ("lon", lon),
        ("source_primary", source_primary),
        ("source_secondary", source_secondary),
        ("external_id", external_id),
        ("external_id_secondary", external_id_secondary),
        ("dedupe_key", dedupe_key),
        ("status", status),
        ("map_url", map_url),
        ("payload_json", _json_dumps(payload)),
    ])

    field_names = ", ".join(name for name, _ in insert_fields)
    placeholders = ", ".join("?" for _ in insert_fields)
    values = [value for _, value in insert_fields]
    cur = con.execute(
        f"INSERT INTO disaster_events ({field_names}) VALUES ({placeholders})",
        values,
    )
    return int(cur.lastrowid), True


def upsert_disaster_ongoing_group(
    con: sqlite3.Connection,
    *,
    group_key: str,
    event_type: str,
    source_primary: str,
    source_secondary: str | None,
    title: str,
    summary: str | None,
    severity_level: str | None,
    severity_color: str | None,
    status: str | None,
    started_at: str | None,
    updated_at: str | None,
    location_text: str | None,
    lat: float | None,
    lon: float | None,
    country_code: str | None,
    map_url: str | None,
    official_link: str | None,
    payload: dict | None,
) -> tuple[int, bool]:
    existing = con.execute(
        "SELECT id FROM disaster_ongoing_groups WHERE group_key = ?",
        (group_key,),
    ).fetchone()

    if existing:
        con.execute(
            """
            UPDATE disaster_ongoing_groups
            SET event_type = ?, source_primary = ?, source_secondary = ?, title = ?, summary = ?,
                severity_level = ?, severity_color = ?, status = ?, started_at = ?, updated_at = ?,
                location_text = ?, lat = ?, lon = ?, country_code = ?, map_url = ?, official_link = ?,
                payload_json = ?
            WHERE id = ?
            """,
            (
                event_type,
                source_primary,
                source_secondary,
                title,
                summary,
                severity_level,
                severity_color,
                status,
                started_at,
                updated_at,
                location_text,
                lat,
                lon,
                country_code,
                map_url,
                official_link,
                _json_dumps(payload),
                int(existing["id"]),
            ),
        )
        return int(existing["id"]), False

    cur = con.execute(
        """
        INSERT INTO disaster_ongoing_groups (
            group_key, event_type, source_primary, source_secondary, title, summary,
            severity_level, severity_color, status, started_at, updated_at, location_text,
            lat, lon, country_code, map_url, official_link, payload_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            group_key,
            event_type,
            source_primary,
            source_secondary,
            title,
            summary,
            severity_level,
            severity_color,
            status,
            started_at,
            updated_at,
            location_text,
            lat,
            lon,
            country_code,
            map_url,
            official_link,
            _json_dumps(payload),
        ),
    )
    return int(cur.lastrowid), True


def replace_disaster_ongoing_members(
    con: sqlite3.Connection,
    *,
    group_id: int,
    event_ids: list[int],
) -> None:
    con.execute("DELETE FROM disaster_ongoing_members WHERE group_id = ?", (group_id,))
    for event_id in event_ids:
        con.execute(
            "INSERT OR IGNORE INTO disaster_ongoing_members (group_id, event_id) VALUES (?, ?)",
            (group_id, int(event_id)),
        )


def clear_disaster_ongoing(con: sqlite3.Connection) -> None:
    con.execute("DELETE FROM disaster_ongoing_members")
    con.execute("DELETE FROM disaster_ongoing_groups")


def insert_swpc_alert(
    con: sqlite3.Connection,
    *,
    dedupe_key: str,
    product_id: str | None,
    message_code: str | None,
    serial_number: str | None,
    issue_datetime: str,
    issue_time_text: str | None,
    headline: str,
    message_type: str | None,
    noaa_scale: str | None,
    details: dict | None,
    impacts_text: str | None,
    description_text: str | None,
    message_raw: str,
    source_name: str,
    source_url: str,
    fetched_at: str,
) -> int:
    cur = con.execute(
        """
        INSERT INTO swpc_alerts (
            dedupe_key, product_id, message_code, serial_number, issue_datetime, issue_time_text,
            headline, message_type, noaa_scale, details_json, impacts_text, description_text,
            message_raw, source_name, source_url, fetched_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            dedupe_key,
            product_id,
            message_code,
            serial_number,
            issue_datetime,
            issue_time_text,
            headline,
            message_type,
            noaa_scale,
            _json_dumps(details),
            impacts_text,
            description_text,
            message_raw,
            source_name,
            source_url,
            fetched_at,
        ),
    )
    return int(cur.lastrowid)


def insert_swpc_forecast(
    con: sqlite3.Connection,
    *,
    forecast_key: str,
    forecast_issued_at: str | None,
    geomag_issued_at: str | None,
    panel: dict,
    raw_forecast_text: str,
    raw_geomag_text: str,
    source_name: str,
    source_url: str,
    fetched_at: str,
) -> int:
    cur = con.execute(
        """
        INSERT INTO swpc_forecasts (
            forecast_key, forecast_issued_at, geomag_issued_at, panel_json,
            raw_forecast_text, raw_geomag_text, source_name, source_url, fetched_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            forecast_key,
            forecast_issued_at,
            geomag_issued_at,
            _json_dumps(panel),
            raw_forecast_text,
            raw_geomag_text,
            source_name,
            source_url,
            fetched_at,
        ),
    )
    return int(cur.lastrowid)



def insert_space_launch_snapshot(
    con: sqlite3.Connection,
    *,
    snapshot_key: str,
    items: list[dict],
    source_name: str,
    source_url: str,
    fetched_at: str,
) -> int:
    cur = con.execute(
        """
        INSERT INTO space_launch_snapshots (
            snapshot_key, items_json, source_name, source_url, fetched_at
        )
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            snapshot_key,
            _json_dumps(items),
            source_name,
            source_url,
            fetched_at,
        ),
    )
    return int(cur.lastrowid)


def upsert_public_health_event(
    con: sqlite3.Connection,
    *,
    dedupe_key: str,
    source_key: str,
    source_name: str,
    category_key: str,
    title_raw: str,
    title_zh: str,
    date_raw: str,
    published_at: str | None,
    item_url: str,
    list_url: str,
    rank: int | None,
    payload,
    translation_provider: str | None,
    fetched_at: str,
) -> tuple[int, bool]:
    existing = con.execute(
        "SELECT id FROM public_health_events WHERE dedupe_key = ?",
        (dedupe_key,),
    ).fetchone()
    is_new = existing is None

    con.execute(
        """
        INSERT INTO public_health_events (
            dedupe_key, source_key, source_name, category_key,
            title_raw, title_zh, date_raw, published_at,
            item_url, list_url, rank, translation_provider,
            payload_json, fetched_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(dedupe_key) DO UPDATE SET
            source_key = excluded.source_key,
            source_name = excluded.source_name,
            category_key = excluded.category_key,
            title_raw = excluded.title_raw,
            title_zh = excluded.title_zh,
            date_raw = excluded.date_raw,
            published_at = excluded.published_at,
            item_url = excluded.item_url,
            list_url = excluded.list_url,
            rank = excluded.rank,
            translation_provider = excluded.translation_provider,
            payload_json = excluded.payload_json,
            fetched_at = excluded.fetched_at
        """,
        (
            dedupe_key,
            source_key,
            source_name,
            category_key,
            title_raw,
            title_zh,
            date_raw,
            published_at,
            item_url,
            list_url,
            rank,
            translation_provider,
            _json_dumps(payload),
            fetched_at,
        ),
    )

    row = con.execute(
        "SELECT id FROM public_health_events WHERE dedupe_key = ?",
        (dedupe_key,),
    ).fetchone()
    return int(row["id"]), is_new

def upsert_news_timeline_item(
    con: sqlite3.Connection,
    *,
    source_key: str,
    source_name: str,
    source_id: int | None,
    channel: str | None,
    topic: str | None,
    title: str,
    url: str,
    published_at: str | None,
    published_text: str | None,
    author_names: list[str] | None,
    list_url: str | None,
    payload,
    fetched_at: str,
) -> tuple[int, bool]:
    dedupe_key = f"{source_key}:{url}"
    existing = con.execute(
        "SELECT id FROM news_timeline_items WHERE dedupe_key = ?",
        (dedupe_key,),
    ).fetchone()
    is_new = existing is None

    con.execute(
        """
        INSERT INTO news_timeline_items (
            dedupe_key, source_id, source_key, source_name, channel, topic,
            title, url, published_at, published_text, author_names_json,
            list_url, payload_json, fetched_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(dedupe_key) DO UPDATE SET
            source_id = excluded.source_id,
            source_key = excluded.source_key,
            source_name = excluded.source_name,
            channel = excluded.channel,
            topic = excluded.topic,
            title = excluded.title,
            url = excluded.url,
            published_at = excluded.published_at,
            published_text = excluded.published_text,
            author_names_json = excluded.author_names_json,
            list_url = excluded.list_url,
            payload_json = excluded.payload_json,
            fetched_at = excluded.fetched_at
        """,
        (
            dedupe_key,
            source_id,
            source_key,
            source_name,
            channel,
            topic,
            title,
            url,
            published_at,
            published_text,
            _json_dumps(author_names or []),
            list_url,
            _json_dumps(payload),
            fetched_at,
        ),
    )

    row = con.execute(
        "SELECT id FROM news_timeline_items WHERE dedupe_key = ?",
        (dedupe_key,),
    ).fetchone()
    return int(row["id"]), is_new


def insert_mnd_pla_daily(
    con: sqlite3.Connection,
    *,
    report_date: str | None,
    published_date_raw: str | None,
    period_start: str | None,
    period_end: str | None,
    report_period_raw: str | None,
    title: str,
    post_url: str,
    body: str,
    activity_text: str,
    no_aircraft: bool,
    aircraft_total: int | None,
    aircraft_intrusion_total: int | None,
    ship_total: int | None,
    official_ship_total: int | None,
    balloon_total: int | None,
    intrusion_areas: list[str] | None,
    source_name: str,
    source_url: str,
    fetched_at: str,
) -> int:
    cur = con.execute(
        """
        INSERT INTO mnd_pla_daily (
            report_date, published_date_raw, period_start, period_end, report_period_raw,
            title, post_url, body, activity_text, no_aircraft,
            aircraft_total, aircraft_intrusion_total, ship_total, official_ship_total, balloon_total,
            intrusion_areas_json, source_name, source_url, fetched_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            report_date,
            published_date_raw,
            period_start,
            period_end,
            report_period_raw,
            title,
            post_url,
            body,
            activity_text,
            int(bool(no_aircraft)),
            aircraft_total,
            aircraft_intrusion_total,
            ship_total,
            official_ship_total,
            balloon_total,
            _json_dumps(intrusion_areas or []),
            source_name,
            source_url,
            fetched_at,
        ),
    )
    return int(cur.lastrowid)


def record_fetch_run(
    con: sqlite3.Connection,
    source_id: int | None,
    source_name: str,
    started_at: str,
    finished_at: str,
    status: str,
    items_found: int,
    new_items: int,
    error_message: str = "",
) -> None:
    con.execute(
        """
        INSERT INTO fetch_runs (
            source_id, source_name, started_at, finished_at, status, items_found, new_items, error_message
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            source_id,
            source_name,
            started_at,
            finished_at,
            status,
            items_found,
            new_items,
            error_message,
        ),
    )


def cleanup_old_data(
    con: sqlite3.Connection,
    *,
    telegram_hours: int = 24,
    media_hours: int = 24,
    energy_hours: int = 24,
    market_snapshot_hours: int = 24,
    iflow_snapshot_hours: int = 72,
    airport_alert_hours: int = 48,
    airport_disruption_hours: int = 24,
    weather_alert_hours: int = 24,
    disaster_hours: int = 168,
    fetch_runs_hours: int = 24 * 7,
    legacy_oil_hours: int = 24 * 7,
    space_weather_hours: int = 24,
    public_health_hours: int = 24 * 30,
    public_health_early_warning_hours: int = 24 * 3,
    public_health_outbreak_keep_years: int = 2,
    news_timeline_hours: int = 24,
    f1_live_latest_gp_hours: int = 24,
    f1_live_latest_gp_max_snapshots: int = 180,
) -> dict:
    stats = {
        "item_media_deleted": 0,
        "items_deleted": 0,
        "energy_quotes_deleted": 0,
        "market_snapshots_deleted": 0,
        "iflow_snapshots_deleted": 0,
        "airport_alerts_deleted": 0,
        "airport_disruptions_deleted": 0,
        "weather_alerts_deleted": 0,
        "disaster_events_deleted": 0,
        "disaster_ongoing_members_deleted": 0,
        "disaster_ongoing_groups_deleted": 0,
        "fetch_runs_deleted": 0,
        "legacy_oil_prices_deleted": 0,
        "f1_live_snapshots_deleted": 0,
        "f1_news_articles_deleted": 0,
        "swpc_alerts_deleted": 0,
        "swpc_forecasts_deleted": 0,
        "mnd_pla_daily_deleted": 0,
        "public_health_events_deleted": 0,
        "news_timeline_items_deleted": 0,
    }

    if _table_exists(con, "item_media"):
        cur = con.execute(
            f"""
            DELETE FROM item_media
            WHERE datetime(created_at) < datetime('now', '-{media_hours} hours')
               OR item_id IN (
                    SELECT id FROM items
                    WHERE datetime(fetched_at) < datetime('now', '-{telegram_hours} hours')
               )
            """
        )
        stats["item_media_deleted"] = cur.rowcount

    if _table_exists(con, "items"):
        cur = con.execute(
            f"DELETE FROM items WHERE datetime(fetched_at) < datetime('now', '-{telegram_hours} hours')"
        )
        stats["items_deleted"] = cur.rowcount

    if _table_exists(con, "energy_quotes"):
        cur = con.execute(
            f"DELETE FROM energy_quotes WHERE datetime(fetched_at) < datetime('now', '-{energy_hours} hours')"
        )
        stats["energy_quotes_deleted"] = cur.rowcount

    if _table_exists(con, "market_snapshots"):
        cur = con.execute(
            f"DELETE FROM market_snapshots WHERE datetime(fetched_at) < datetime('now', '-{market_snapshot_hours} hours')"
        )
        stats["market_snapshots_deleted"] = cur.rowcount

    if _table_exists(con, "iflow_snapshots"):
        cur = con.execute(
            f"DELETE FROM iflow_snapshots WHERE datetime(fetched_at) < datetime('now', '-{iflow_snapshot_hours} hours')"
        )
        stats["iflow_snapshots_deleted"] = cur.rowcount

    if _table_exists(con, "airport_alerts"):
        cur = con.execute(
            f"DELETE FROM airport_alerts WHERE datetime(fetched_at) < datetime('now', '-{airport_alert_hours} hours')"
        )
        stats["airport_alerts_deleted"] = cur.rowcount

    if _table_exists(con, "airport_disruptions"):
        cur = con.execute(
            f"DELETE FROM airport_disruptions WHERE datetime(fetched_at) < datetime('now', '-{airport_disruption_hours} hours')"
        )
        stats["airport_disruptions_deleted"] = cur.rowcount

    if _table_exists(con, "weather_alerts"):
        cur = con.execute(
            f"DELETE FROM weather_alerts WHERE datetime(fetched_at) < datetime('now', '-{weather_alert_hours} hours')"
        )
        stats["weather_alerts_deleted"] = cur.rowcount

    if _table_exists(con, "disaster_ongoing_members"):
        cur = con.execute(
            f"""
            DELETE FROM disaster_ongoing_members
            WHERE group_id IN (
                SELECT id FROM disaster_ongoing_groups
                WHERE datetime(COALESCE(updated_at, created_at)) < datetime('now', '-{disaster_hours} hours')
            )
            """
        )
        stats["disaster_ongoing_members_deleted"] = cur.rowcount

    if _table_exists(con, "disaster_ongoing_groups"):
        cur = con.execute(
            f"DELETE FROM disaster_ongoing_groups WHERE datetime(COALESCE(updated_at, created_at)) < datetime('now', '-{disaster_hours} hours')"
        )
        stats["disaster_ongoing_groups_deleted"] = cur.rowcount

    if _table_exists(con, "disaster_events"):
        cur = con.execute(
            f"DELETE FROM disaster_events WHERE datetime(fetched_at) < datetime('now', '-{disaster_hours} hours')"
        )
        stats["disaster_events_deleted"] = cur.rowcount

    if _table_exists(con, "fetch_runs"):
        cur = con.execute(
            f"DELETE FROM fetch_runs WHERE datetime(started_at) < datetime('now', '-{fetch_runs_hours} hours')"
        )
        stats["fetch_runs_deleted"] = cur.rowcount

    if _table_exists(con, "swpc_alerts"):
        cur = con.execute(
            f"DELETE FROM swpc_alerts WHERE datetime(fetched_at) < datetime('now', '-{space_weather_hours} hours')"
        )
        stats["swpc_alerts_deleted"] = cur.rowcount

    if _table_exists(con, "swpc_forecasts"):
        cur = con.execute(
            f"DELETE FROM swpc_forecasts WHERE datetime(fetched_at) < datetime('now', '-{space_weather_hours} hours')"
        )
        stats["swpc_forecasts_deleted"] = cur.rowcount

    if _table_exists(con, "public_health_events"):
        cur_early = con.execute(
            f"""
            DELETE FROM public_health_events
            WHERE category_key = 'early_warning'
              AND datetime(COALESCE(published_at, fetched_at, created_at)) < datetime('now', '-{public_health_early_warning_hours} hours')
            """
        )
        cur_outbreak = con.execute(
            f"""
            DELETE FROM public_health_events
            WHERE category_key = 'outbreak_event'
              AND CAST(strftime('%Y', COALESCE(published_at, fetched_at, created_at)) AS INTEGER) < CAST(strftime('%Y', 'now') AS INTEGER) - {public_health_outbreak_keep_years - 1}
            """
        )
        cur_other = con.execute(
            f"""
            DELETE FROM public_health_events
            WHERE category_key NOT IN ('early_warning', 'outbreak_event')
              AND datetime(COALESCE(published_at, fetched_at, created_at)) < datetime('now', '-{public_health_hours} hours')
            """
        )
        stats["public_health_events_deleted"] = cur_early.rowcount + cur_outbreak.rowcount + cur_other.rowcount

    if _table_exists(con, "news_timeline_items"):
        cur = con.execute(
            f"""
            DELETE FROM news_timeline_items
            WHERE datetime(COALESCE(published_at, fetched_at, created_at)) < datetime('now', '-{news_timeline_hours} hours')
            """
        )
        stats["news_timeline_items_deleted"] = cur.rowcount

    if _table_exists(con, "mnd_pla_daily"):
        cur = con.execute(
            """
            DELETE FROM mnd_pla_daily
            WHERE report_date NOT IN (
                SELECT report_date
                FROM mnd_pla_daily
                ORDER BY report_date DESC
                LIMIT 7
            )
            """
        )
        stats["mnd_pla_daily_deleted"] = cur.rowcount

    if _table_exists(con, "f1_live_snapshots"):
        latest_gp = con.execute(
            """
            SELECT gp_name
            FROM f1_live_snapshots
            ORDER BY COALESCE(fetched_at, created_at) DESC, id DESC
            LIMIT 1
            """
        ).fetchone()
        if latest_gp and latest_gp["gp_name"]:
            deleted_total = 0
            cur = con.execute(
                "DELETE FROM f1_live_snapshots WHERE gp_name <> ?",
                (latest_gp["gp_name"],),
            )
            deleted_total += cur.rowcount
            cur = con.execute(
                f"""
                DELETE FROM f1_live_snapshots
                WHERE gp_name = ?
                  AND datetime(COALESCE(fetched_at, created_at)) < datetime('now', '-{f1_live_latest_gp_hours} hours')
                """ ,
                (latest_gp["gp_name"],),
            )
            deleted_total += cur.rowcount
            cur = con.execute(
                """
                DELETE FROM f1_live_snapshots
                WHERE gp_name = ?
                  AND id NOT IN (
                      SELECT id
                      FROM f1_live_snapshots
                      WHERE gp_name = ?
                      ORDER BY COALESCE(fetched_at, created_at) DESC, id DESC
                      LIMIT ?
                  )
                """,
                (latest_gp["gp_name"], latest_gp["gp_name"], f1_live_latest_gp_max_snapshots),
            )
            deleted_total += cur.rowcount
            stats["f1_live_snapshots_deleted"] = deleted_total

    if _table_exists(con, "f1_news_articles"):
        cur = con.execute(
            """
            DELETE FROM f1_news_articles
            WHERE id NOT IN (
                SELECT id
                FROM f1_news_articles
                ORDER BY COALESCE(published_at, fetched_at) DESC, id DESC
                LIMIT 20
            )
            """
        )
        stats["f1_news_articles_deleted"] = cur.rowcount

    if _table_exists(con, "oil_prices"):
        cur = con.execute(
            f"DELETE FROM oil_prices WHERE datetime(fetched_at) < datetime('now', '-{legacy_oil_hours} hours')"
        )
        stats["legacy_oil_prices_deleted"] = cur.rowcount

    return stats


def cleanup_orphaned_media_files(
    con: sqlite3.Connection,
    *,
    media_root: str | Path = Path("raw") / "media" / "images",
    delete_zero_byte: bool = True,
    prune_empty_dirs: bool = True,
    dry_run: bool = False,
) -> dict:
    root = Path(media_root)
    if not root.is_absolute():
        root = (PROJECT_ROOT / root).resolve()

    stats = {
        "media_root_exists": root.exists(),
        "files_seen": 0,
        "files_kept": 0,
        "orphan_files_deleted": 0,
        "zero_byte_deleted": 0,
        "delete_errors": 0,
        "bytes_freed": 0,
        "empty_dirs_deleted": 0,
    }

    if not root.exists():
        return stats

    referenced_paths: set[Path] = set()

    if _table_exists(con, "item_media"):
        rows = con.execute(
            """
            SELECT local_path
            FROM item_media
            WHERE local_path IS NOT NULL
              AND TRIM(local_path) <> ''
              AND download_status = 'downloaded'
            """
        ).fetchall()

        for row in rows:
            raw = str(row["local_path"]).strip()
            if not raw:
                continue
            p = Path(raw)
            referenced_paths.add(p)
            try:
                referenced_paths.add(p.resolve())
            except Exception:
                pass
            if not p.is_absolute():
                try:
                    referenced_paths.add((PROJECT_ROOT / p).resolve())
                except Exception:
                    pass

    for file_path in root.rglob("*"):
        if not file_path.is_file():
            continue
        stats["files_seen"] += 1
        try:
            file_size = file_path.stat().st_size
        except Exception:
            file_size = 0

        keep = False
        if file_path in referenced_paths:
            keep = True
        else:
            try:
                if file_path.resolve() in referenced_paths:
                    keep = True
            except Exception:
                pass

        if keep:
            stats["files_kept"] += 1
            continue

        try:
            if not dry_run:
                file_path.unlink(missing_ok=True)
            if file_size == 0 and delete_zero_byte:
                stats["zero_byte_deleted"] += 1
            else:
                stats["orphan_files_deleted"] += 1
            stats["bytes_freed"] += max(file_size, 0)
        except Exception:
            stats["delete_errors"] += 1

    if prune_empty_dirs:
        dirs = [p for p in root.rglob("*") if p.is_dir()]
        dirs.sort(key=lambda p: len(p.parts), reverse=True)
        for dir_path in dirs:
            try:
                next(dir_path.iterdir())
            except StopIteration:
                try:
                    if not dry_run:
                        dir_path.rmdir()
                    stats["empty_dirs_deleted"] += 1
                except Exception:
                    pass
            except Exception:
                pass

    return stats

def _table_columns(con: sqlite3.Connection, table_name: str) -> set[str]:
    rows = con.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {str(row[1]) for row in rows}

