from pathlib import Path
from tqdm import tqdm
from persistence import fetch_downloaded, mark_failed, mark_transcribed
from typing import Tuple, List
from sqlite3 import Connection
from faster_whisper import WhisperModel
from faster_whisper.transcribe import Segment

DATA_DIR = Path("data")
MODEL_NAME = "small"
DEVICE = "auto"
COMPUTE_TYPE = "auto"
BATCH_SIZE = 6

MODEL = WhisperModel(
    model_size_or_path=MODEL_NAME,
    device=DEVICE,
    compute_type=COMPUTE_TYPE,
)


def transcribe_video(video_path: str) -> List[Segment]:
    segments, info = MODEL.transcribe(
        video_path,
        language="en",
        beam_size=1,
        vad_filter=True,
        vad_parameters={"min_silence_duration_ms": 1000},
    )
    all_segments: List[Segment] = []
    total = info.duration

    with tqdm(
            total=total,
            unit="sec",
            desc=f"Transcribing {video_path}",
            dynamic_ncols=True,
    ) as pbar:
        for seg in segments:
            all_segments.append(seg)
            pbar.update(seg.end - seg.start)

    return all_segments


def _srt_timestamp(seconds: float) -> str:
    ms = int(round(seconds * 1000))
    hh, rem = divmod(ms, 3_600_000)
    mm, rem = divmod(rem, 60_000)
    ss, ms = divmod(rem, 1000)
    return f"{hh:02d}:{mm:02d}:{ss:02d},{ms:03d}"


def write_srt(segments: List[Segment], source: str, title: str) -> Path:
    out_dir = DATA_DIR / source / title
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "transcript.srt"

    tmp = out_path.with_suffix(out_path.suffix + ".part")

    idx = 1
    with tmp.open("w", encoding="utf-8", newline="\n") as f:
        for seg in segments:
            text = (seg.text or "").strip()
            if not text:
                continue

            start = _srt_timestamp(seg.start)
            end = _srt_timestamp(seg.end)
            f.write(f"{idx}\n{start} --> {end}\n{text}\n\n")
            idx += 1

    tmp.replace(out_path)
    return out_path


def transcribe_videos(conn: Connection) -> Tuple[int, int]:
    rows = fetch_downloaded(conn, limit=BATCH_SIZE)
    if not rows:
        print("No videos to transcribe.")
        return 0, 0

    print(f"Transcribing {len(rows)} videos…")

    successes = 0
    failures = 0

    for row in rows:
        video_path = row["download_path"]
        source = row["source"]
        external_id = row["external_id"]
        title = row["title"]

        try:
            segments = transcribe_video(video_path)
            srt_path = write_srt(segments, source, title)
            mark_transcribed(conn, source, external_id, str(srt_path))
            successes += 1
            tqdm.write(f"✓ {source}/{title} → {srt_path}")

        except Exception as e:
            mark_failed(conn, source, external_id, str(e))
            failures += 1
            tqdm.write(f"✗ {source}/{title} failed: {e}")

    return successes, failures
