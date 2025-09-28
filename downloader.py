import requests, time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from persistence import fetch_videos_to_download, mark_failed, mark_downloaded, connect, init_db
from sqlite3 import Connection, Row
from typing import Tuple, Union

DATA_DIR = Path("./data")
CHUNK_SIZE = 8 * 1024 * 1024
MAX_WORKERS = 3
BATCH_SIZE = 6
GRACE_SECONDS = 15
MIN_RATE_BPS = 2 * 1024 * 1024


def get_direct_mp4_url(source: str, external_id: str) -> str:
    if source == "house":
        return f"https://www.house.mi.gov/ArchiveVideoFiles/{external_id}.mp4"
    else:
        return f"https://dlttx48mxf9m3.cloudfront.net/outputs/{external_id}/Default/MP4/out_1080.mp4"


def download_to_local(video_url: str, source: str, title: str, position: int = 0) -> Path:
    dest = DATA_DIR / source / title / "video.mp4"
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    start = time.monotonic()

    try:
        with requests.get(video_url, stream=True, timeout=300, verify=False) as response:
            response.raise_for_status()
            content_length = response.headers.get("content-length")
            total_size = int(content_length) if content_length else 0
            desc = f"{source}/{title}"

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

                        if time.monotonic() - start >= GRACE_SECONDS:
                            rate_bps = pbar.format_dict.get("rate") or 0.0
                            if rate_bps and rate_bps < MIN_RATE_BPS:
                                raise RuntimeError(f"Download too slow: {rate_bps/1_048_576:.2f} MB/s")

        tmp.replace(dest)
        return dest

    except Exception:
        tmp.unlink(missing_ok=True)
        raise


def download_one(row: Row, position: int = 0) -> Tuple[str, str, str, Union[Path, Exception]]:
    source = row["source"]
    external_id = row["external_id"]
    title = row["title"]
    mp4_url = get_direct_mp4_url(source, external_id)

    try:
        dest = download_to_local(mp4_url, source, title, position)
        return source, external_id, title, dest

    except Exception as e:
        return source, external_id, title, e


def download_concurrent(conn: Connection) -> tuple[int, int]:
    rows = fetch_videos_to_download(conn, BATCH_SIZE)
    if not rows:
        print("No videos to download.")
        return 0, 0

    print(f"Downloading {len(rows)} videos…")

    successes = 0
    failures = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(download_one, row, pos) for pos, row in enumerate(rows)]

        for future in as_completed(futures):
            source, external_id, title, result = future.result()

            if isinstance(result, Exception):
                mark_failed(conn, source, external_id, str(result), 'failed_download')
                failures += 1
                tqdm.write(f"✗ {source}/{title} failed: {result}")
            else:
                mark_downloaded(conn, source, external_id, str(result))
                successes += 1
                tqdm.write(f"✓ {source}/{title} → {result}")

    return successes, failures


if __name__ == "__main__":
    connection = connect()
    init_db(connection)

    vids_to_download = fetch_videos_to_download(connection, BATCH_SIZE)

    for r in vids_to_download:
        print(r["title"], get_direct_mp4_url(r["source"], r["external_id"]))
