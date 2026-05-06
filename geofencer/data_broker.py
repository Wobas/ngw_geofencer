import os
import sqlite3
from datetime import datetime, timezone


DB_PATH = "./app/data/events.db"
TABLE_NAME = "events"


def init_db() -> None:
    """
    Initialize data base for writing events.
    """
    db_dir = os.path.dirname(DB_PATH)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts INTEGER NOT NULL,
            message TEXT NOT NULL,
            geom_1 TEXT NOT NULL,
            geom_2 TEXT NOT NULL
        )
    """)

    cursor.execute(f"""
        CREATE INDEX IF NOT EXISTS idx_events_ts
        ON {TABLE_NAME}(ts)
    """)

    conn.commit()
    conn.close()

def write_event(message: str, geom1_wkt: str, geom2_wkt: str) -> None:
    """
    Write geofence event to the data base.

    Parameters
    ----------
    message : str
        text message info about the event

    geom1_wkt : str
        string with geometry of top layer feature in WKT format

    geom2_wkt : str
        string with geometry of bottom layer feature in WKT format 
    """
    ts = int(datetime.now(timezone.utc).timestamp())

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO events (ts, message, geom_1, geom_2)
        VALUES (?, ?, ?, ?)
    """, (ts, message, geom1_wkt, geom2_wkt))

    conn.commit()
    conn.close()

def read_all_events() -> list[tuple[int, str, str, str]]:
    """
    Get all events from the database ordered by timestamp descending.

    Returns
    -------
    list[tuple[int, str, str, str]]
        list containing all events, where each event is a tuple:
        (ts, message, geom1_wkt, geom2_wkt)
        Returns empty list if no events exist.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute(f"""
        SELECT ts, message, geom_1, geom_2
        FROM {TABLE_NAME}
        ORDER BY ts DESC
    """)

    rows = cursor.fetchall()
    conn.close()

    return rows

def read_last_event() -> tuple[int, str, str, str] | None:
    """
    Get last event from the database ordered by timestamp descending.

    Returns
    -------
    tuple[int, str, str, str]
        containing event tuple:
        (ts, message, geom1_wkt, geom2_wkt)
        Returns None if no events exist.
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute(f"""
        SELECT ts, message, geom_1, geom_2
        FROM {TABLE_NAME}
        ORDER BY ts DESC
        LIMIT 1
    """)

    row = cursor.fetchone()
    conn.close()

    return row

def read_datetime_range_events(start_ts, end_ts):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute(f"""
        SELECT ts, message, geom_1, geom_2
        FROM {TABLE_NAME}
        WHERE ts BETWEEN ? AND ?
        ORDER BY ts DESC
    """, (start_ts, end_ts))

    rows = cursor.fetchall()
    conn.close()

    return rows

def read_last_seconds(seconds_number: int):
    now = int(datetime.now(timezone.utc).timestamp())
    ten_minutes_ago = now - seconds_number

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute(f"""
        SELECT ts, message, geom_1, geom_2
        FROM {TABLE_NAME}
        WHERE ts BETWEEN ? AND ?
        ORDER BY ts DESC
    """, (ten_minutes_ago, now))

    rows = cursor.fetchall()
    conn.close()

    return rows


