import requests, re
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone, date
from urllib.parse import urljoin, urlparse, parse_qs
from typing import List, Optional
from models import Video

HOUSE_ARCHIVE = "https://house.mi.gov/VideoArchive"
UA = "StateAffairsIngest/1.0 (+rrhuang99@gmail.com)"

def _parse_date_from_text(text: str) -> Optional[date]:
    m = re.match(r"^[A-Za-z]+,\s+[A-Za-z]+\s+\d{1,2},\s+\d{4}", text)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(0), "%A, %B %d, %Y").date()
    except ValueError:
        return None

def fetch_house_videos(lookback_days: int) -> List[Video]:
    response = requests.get(HOUSE_ARCHIVE, headers={"User-Agent": UA}, timeout=30, verify=False)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    videos: List[Video] = []
    cutoff = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).date()

    for div in soup.select("div.page-search-object"):
        a = div.select_one("a[href]")
        if not a:
            continue

        title_text = a.get_text(strip=True)
        dt = _parse_date_from_text(title_text)
        if not dt or dt < cutoff:
            continue

        player_url = urljoin(HOUSE_ARCHIVE, a["href"])
        parsed_url = urlparse(player_url)

        video_param = parse_qs(parsed_url.query).get("video", [None])[0]
        if not video_param or not video_param.endswith(".mp4"):
            continue

        external_id = video_param[:-4]
        videos.append(Video(
            source="house",
            external_id=external_id,
            title=external_id,
            date=dt,
            url=player_url,
        ))

    return videos


if __name__ == "__main__":
    vids = fetch_house_videos(lookback_days=30)
    print(f"Found {len(vids)} recent House videos")

    for v in vids:
        print(v.date, v.external_id, v.title, v.url)
