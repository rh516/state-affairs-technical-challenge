import requests
from models import Video
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

DATA_DIR = Path("./data")
CHUNK_SIZE = 8 * 1024 * 1024

MAX_WORKERS = 3
BATCH_SIZE = 6

def fetch_batch(conn, limit: int = BATCH_SIZE):
    return conn.execute(
        """
        SELECT source, external_id, url, date
        FROM videos
        WHERE status IN ('discovered', 'failed')
        ORDER BY date DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()

def get_direct_mp4_url(video: Video) -> str:
    return f"https://www.house.mi.gov/ArchiveVideoFiles/{video.external_id}.mp4"

def download_to_local(video_url: str, source: str, external_id: str, position: int = 0) -> Path:
    dest = DATA_DIR / source / external_id / "video.mp4"
    dest.parent.mkdir(parents=True, exist_ok=True)

    tmp = dest.with_suffix(dest.suffix + ".part")

    try:
        with requests.get(video_url, stream=True, timeout=300, verify=False) as response:
            response.raise_for_status()
            total_size = int(response.headers.get("Content-Length", 0))
            desc = f"{source}/{external_id}"

            with tqdm(
                total=total_size or None,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                desc=desc,
                position=position,
                leave=True,
                dynamic_ncols=True,
            ) as pbar:
                with open(tmp, "wb") as f:
                    for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                        if not chunk:
                            continue
                        f.write(chunk)
                        pbar.update(len(chunk))

    except Exception as e:
        tqdm.write(f"{source}/{external_id} failed: {e}")
        raise

    tmp.replace(dest)
    return dest

def download_one(row, position: int = 0):
    video = Video(
        source=row["source"],
        external_id=row["external_id"],
        url=row["url"],
        date=datetime.fromisoformat(row["date"]).date()
    )
    mp4_url = get_direct_mp4_url(video)
    try:
        dest = download_to_local(mp4_url, video.source, video.external_id, position)
        return video.source, video.external_id, dest
    except Exception as e:
        return video.source, video.external_id, e

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

def download_concurrent(conn):
    rows = fetch_batch(conn, BATCH_SIZE)
    if not rows:
        print("No videos to download.")
        return

    print(f"Downloading {len(rows)} videos with {MAX_WORKERS} workers…")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(download_one, row, pos) for pos, row in enumerate(rows)]

        for future in as_completed(futures):
            source, external_id, result = future.result()

            if isinstance(result, Exception):
                mark_failed(conn, source, external_id)
                tqdm.write(f"✗ {external_id} failed: {result}")
            else:
                mark_downloaded(conn, source, external_id, str(result))
                tqdm.write(f"✓ {external_id} → {result} \n")

if __name__ == "__main__":
    print("Downloading...")

