import requests
import sys
from models import Video
from pathlib import Path
from datetime import datetime

DATA_DIR = Path("./data")
CHUNK_SIZE = 8 * 1024 * 1024

def get_direct_mp4_url(video: Video) -> str:
    return f"https://www.house.mi.gov/ArchiveVideoFiles/{video.external_id}.mp4"

def download_to_local(video_url: str, source: str, external_id: str) -> Path:
    dest = DATA_DIR / source / external_id / "video.mp4"
    dest.parent.mkdir(parents=True, exist_ok=True)

    tmp = dest.with_suffix(dest.suffix + ".part")

    with requests.get(video_url, stream=True, timeout=300, verify=False) as response:
        response.raise_for_status()
        total_size = int(response.headers.get("Content-Length", 0))
        downloaded = 0

        with open(tmp, "wb") as f:
            for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                if not chunk:
                    continue
                f.write(chunk)
                downloaded += len(chunk)

                if total_size:
                    pct = 100 * downloaded / total_size
                    sys.stdout.write(f"\r{downloaded/1_048_576:.1f}MB / {total_size/1_048_576:.1f}MB ({pct:.1f}%)")
                else:
                    sys.stdout.write(f"\r{downloaded/1_048_576:.1f}MB")

                sys.stdout.flush()

    tmp.replace(dest)
    sys.stdout.write("\n")
    return dest

def mark_downloaded(conn, source: str, external_id: str, download_path: str):
    conn.execute(
        """
        UPDATE videos SET status = 'downloaded', download_path = ? 
        WHERE external_id = ? AND source = ?
        """,
        (download_path, external_id, source),
    )
    conn.commit()

def mark_failed(conn, source: str, external_id: str):
    conn.execute(
        "UPDATE videos SET status = 'failed' WHERE external_id = ? AND source = ?",
        (external_id, source),
    )
    conn.commit()

def download_videos(conn):
    rows = conn.execute(
        "SELECT source, external_id, date, url FROM videos WHERE status='discovered'"
    ).fetchall()

    for row in rows:
        video = Video(
            source=row["source"],
            external_id=row["external_id"],
            date=datetime.fromisoformat(row["date"]).date(),
            url=row["url"],
        )
        try:
            print(f"Downloading {video.external_id}")

            mp4_url = get_direct_mp4_url(video)
            dest = download_to_local(mp4_url, video.source, video.external_id)
            mark_downloaded(conn, video.source, video.external_id, str(dest))

            print(f"Saved {video.external_id} to {dest}")

        except Exception as e:
            mark_failed(conn, video.source, video.external_id)
            print(f"Failed {video.external_id}: {e}")

if __name__ == "__main__":
    print("Downloading video")

