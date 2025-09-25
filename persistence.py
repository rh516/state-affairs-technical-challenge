import sqlite3
from pathlib import Path
from typing import List
from models import Video

DB_PATH = Path("state_affairs.db")
SCHEMA = """
    CREATE TABLE IF NOT EXISTS videos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source TEXT NOT NULL,
        external_id TEXT NOT NULL,
        date TEXT NOT NULL,
        url TEXT NOT NULL,
        download_path TEXT,
        status TEXT NOT NULL DEFAULT 'discovered',
        discovered_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (source, external_id)
    )
"""

def connect(db_path: Path = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn

def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    conn.commit()

def upsert_videos(conn: sqlite3.Connection, videos: List[Video]) -> int:
    inserted = 0
    cursor = conn.cursor()

    for v in videos:
        cursor.execute(
            """
            INSERT INTO videos (source, external_id, date, url)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (source, external_id) DO NOTHING
            """,
            (v.source, v.external_id, v.date.isoformat(), v.url),
        )
        if cursor.rowcount == 1:
            inserted += 1

    conn.commit()
    return inserted

def list_all(conn):
    rows = conn.execute("SELECT * FROM videos ORDER BY date DESC").fetchall()
    print(len(rows))
    # for row in rows:
    #     print(dict(row))

if __name__ == "__main__":
    connection = connect()
    init_db(connection)
    print("DB ready")

