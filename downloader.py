import requests
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from persistence import fetch_batch, mark_failed, mark_downloaded

DATA_DIR = Path("./data")
CHUNK_SIZE = 8 * 1024 * 1024

MAX_WORKERS = 3
BATCH_SIZE = 6

def get_direct_mp4_url(external_id: str) -> str:
    return f"https://www.house.mi.gov/ArchiveVideoFiles/{external_id}.mp4"

def download_to_local(video_url: str, source: str, external_id: str, position: int = 0) -> Path:
    dest = DATA_DIR / source / external_id / "video.mp4"
    dest.parent.mkdir(parents=True, exist_ok=True)

    tmp = dest.with_suffix(dest.suffix + ".part")

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

    tmp.replace(dest)
    return dest

def download_one(row, position: int = 0):
    source = row["source"]
    external_id = row["external_id"]
    mp4_url = get_direct_mp4_url(external_id)

    try:
        dest = download_to_local(mp4_url, source, external_id, position)
        return source, external_id, dest

    except Exception as e:
        return source, external_id, e

def download_concurrent(conn) -> tuple[int, int]:
    rows = fetch_batch(conn, BATCH_SIZE)
    if not rows:
        print("No videos to download.")
        return 0, 0

    print(f"Downloading {len(rows)} videos…")

    successes = 0
    failures = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(download_one, row, pos) for pos, row in enumerate(rows)]

        for future in as_completed(futures):
            source, external_id, result = future.result()

            if isinstance(result, Exception):
                mark_failed(conn, source, external_id, str(result))
                failures += 1
                tqdm.write(f"✗ {external_id} failed: {result}")
            else:
                mark_downloaded(conn, source, external_id, str(result))
                successes += 1
                tqdm.write(f"✓ {external_id} → {result} \n")

    return successes, failures
