from typing import Tuple, List
from faster_whisper import WhisperModel
from pathlib import Path
from tqdm import tqdm
from faster_whisper.transcribe import Segment, TranscriptionInfo

DATA_DIR = Path("data")
MODEL_NAME = "small"
DEVICE = "auto"
COMPUTE_TYPE = "auto"

MODEL = WhisperModel(
    model_size_or_path=MODEL_NAME,
    device=DEVICE,
    compute_type=COMPUTE_TYPE,
)

def find_videos(root: Path) -> List[Path]:
    return [p for p in root.glob("*/*/video.mp4") if p.is_file()]

def transcribe_video(mp4_path: Path) -> Tuple[List[Segment], TranscriptionInfo]:
    segments, info = MODEL.transcribe(
        str(mp4_path),
        language="en",
        beam_size=1,
        vad_filter=True,
        vad_parameters={"min_silence_duration_ms": 1000},
    )
    total = info.duration
    pbar = tqdm(total=total, unit="sec", desc=f"Transcribing {mp4_path.name}")

    all_segments = []
    for seg in segments:
        all_segments.append(seg)
        pbar.update(seg.end - seg.start)

    pbar.close()
    return all_segments, info

def _srt_timestamp(seconds: float) -> str:
    ms = int(round(seconds * 1000))
    hh, rem = divmod(ms, 3_600_000)
    mm, rem = divmod(rem, 60_000)
    ss, ms = divmod(rem, 1000)
    return f"{hh:02d}:{mm:02d}:{ss:02d},{ms:03d}"

def write_srt(segments: List[Segment], source: str, external_id: str) -> Path:
    out_dir = DATA_DIR / source / external_id
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "transcript.srt"

    tmp = out_path.with_suffix(out_path.suffix + ".part")

    with tmp.open("w", encoding="utf-8", newline="\n") as f:
        for idx, seg in enumerate(segments, start=1):
            start = _srt_timestamp(seg.start)
            end = _srt_timestamp(seg.end)
            text = (seg.text or "").strip()

            if not text:
                continue

            f.write(f"{idx}\n{start} --> {end}\n{text}\n\n")

    tmp.replace(out_path)
    return out_path

def transcribe_all(root: Path) -> None:
    pass

if __name__ == "__main__":
    videos = find_videos(DATA_DIR)
    print(videos)
    print("Found {} videos".format(len(videos)))

    print(videos[1], videos[1].parent.name, videos[1].parent.parent.name)

    # s, i = transcribe_video(videos[1])
    # out = write_srt(
    #     s,
    #     videos[1].parent.parent.name,
    #     videos[1].parent.name
    # )
    # print(out)

