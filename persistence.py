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
        title TEXT NOT NULL,
        date TEXT NOT NULL,
        url TEXT NOT NULL,
        download_path TEXT,
        transcript_path TEXT,
        status TEXT NOT NULL DEFAULT 'discovered',
        discovered_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        error TEXT,
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
            INSERT INTO videos (source, external_id, title, date, url)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (source, external_id) DO NOTHING
            """,
            (v.source, v.external_id, v.title, v.date.isoformat(), v.url),
        )
        if cursor.rowcount == 1:
            inserted += 1

    conn.commit()
    return inserted

def mark_downloaded(conn, source: str, external_id: str, download_path: str):
    conn.execute(
        """
        UPDATE videos SET status = 'downloaded', download_path = ? 
        WHERE external_id = ? AND source = ?
        """,
        (download_path, external_id, source),
    )
    conn.commit()

def mark_failed(conn, source: str, external_id: str, error: str):
    conn.execute(
        """
        UPDATE videos SET status = 'failed', error = ?
        WHERE external_id = ? AND source = ?
        """,
        (error, external_id, source),
    )
    conn.commit()

def mark_transcribed(conn, source: str, external_id: str, transcript_path: str):
    conn.execute(
        """
        UPDATE videos SET status = 'transcribed', transcript_path = ?
        WHERE external_id = ? AND source = ?
        """,
        (transcript_path, external_id, source)
    )
    conn.commit()

def fetch_batch(conn, limit: int):
    return conn.execute(
        """
        SELECT source, external_id, title, url, date
        FROM videos
        WHERE status IN ('discovered', 'failed')
        ORDER BY date DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()

def fetch_downloaded(conn, limit: int):
    return conn.execute(
        """
        SELECT source, external_id, title, download_path FROM videos
        WHERE status = 'downloaded' AND download_path IS NOT NULL
        ORDER BY date DESC
        LIMIT ?
        """,
        (limit,)
    ).fetchall()

if __name__ == "__main__":
    connection = connect()
    init_db(connection)
    print("DB ready")

