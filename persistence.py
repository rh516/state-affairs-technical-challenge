import sqlite3
from sqlite3 import Connection, Row
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


def connect(db_path: Path = DB_PATH) -> Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def init_db(conn: Connection) -> None:
    conn.executescript(SCHEMA)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_videos_status ON videos(status)")
    conn.commit()


def upsert_videos(conn: Connection, videos: List[Video]) -> int:
    inserted = 0
    cursor = conn.cursor()

    for v in videos:
        cursor.execute(
            """
            INSERT INTO videos (source, external_id, title, date, url)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (source, external_id) DO NOTHING
            """,
            [v.source, v.external_id, v.title, v.date, v.url],
        )
        if cursor.rowcount == 1:
            inserted += 1

    conn.commit()
    return inserted


def mark_downloaded(conn: Connection, source: str, external_id: str, download_path: str) -> None:
    conn.execute(
        """
        UPDATE videos SET status = 'downloaded', download_path = ? 
        WHERE external_id = ? AND source = ?
        """,
        [download_path, external_id, source],
    )
    conn.commit()


def mark_failed(conn: Connection, source: str, external_id: str, error: str) -> None:
    conn.execute(
        """
        UPDATE videos SET status = 'failed', error = ?
        WHERE external_id = ? AND source = ?
        """,
        [error, external_id, source],
    )
    conn.commit()


def mark_transcribed(conn: Connection, source: str, external_id: str, transcript_path: str) -> None:
    conn.execute(
        """
        UPDATE videos SET status = 'transcribed', transcript_path = ?
        WHERE external_id = ? AND source = ?
        """,
        [transcript_path, external_id, source],
    )
    conn.commit()


def fetch_batch(conn: Connection, limit: int) -> List[Row]:
    return conn.execute(
        """
        SELECT source, external_id, title, url, date
        FROM videos
        WHERE status IN ('discovered', 'failed')
        ORDER BY date DESC
        LIMIT ?
        """,
        [limit],
    ).fetchall()


def fetch_downloaded(conn: Connection, limit: int) -> List[Row]:
    return conn.execute(
        """
        SELECT source, external_id, title, download_path FROM videos
        WHERE status = 'downloaded' AND download_path IS NOT NULL
        ORDER BY date DESC
        LIMIT ?
        """,
        [limit],
    ).fetchall()


if __name__ == "__main__":
    connection = connect()
    init_db(connection)
    print("DB ready")
