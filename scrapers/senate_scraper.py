import requests
from datetime import datetime, timedelta, timezone
from typing import List
from models import Video

SENATE_URL = "https://2kbyogxrg4.execute-api.us-west-2.amazonaws.com/61b3adc8124d7d000891ca5c/home/recent"
HEADERS = {
    "User-Agent": "StateAffairsIngest/1.0 (+rrhuang99@gmail.com)"
}


def get_video_player_url(external_id: str) -> str:
    return f"https://cloud.castus.tv/vod/misenate/video/{external_id}?page=HOME"


def fetch_senate_videos(lookback_days: int) -> List[Video]:
    response = requests.get(SENATE_URL, headers=HEADERS, timeout=30)
    response.raise_for_status()
    data = response.json()

    videos: List[Video] = []
    cutoff = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).date()

    for item in data:
        if "_id" not in item or "date" not in item:
            continue

        try:
            dt = datetime.fromisoformat(item["date"].replace("Z", "+00:00")).date()
        except ValueError:
            continue

        if dt < cutoff:
            continue

        external_id = item["_id"]
        title = item.get("metadata", {}).get("filename") or external_id
        url = get_video_player_url(external_id)

        videos.append(Video(
            source="senate",
            external_id=external_id,
            title=title,
            date=dt,
            url=url,
        ))

    return videos


if __name__ == "__main__":
    senate_videos = fetch_senate_videos(lookback_days=7)
    print(f"Found {len(senate_videos)} recent senate videos")

    for video in senate_videos:
        print(video.source, video.external_id, video.title, video.date, video.url)
