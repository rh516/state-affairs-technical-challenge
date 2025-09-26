import requests, re
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin, urlparse, parse_qs
from typing import List
from models import Video

HOUSE_ARCHIVE = "https://house.mi.gov/VideoArchive"
UA = "StateAffairsIngest/1.0 (+rrhuang99@gmail.com)"

def fetch_house_videos(lookback_days: int) -> List[Video]:
    response = requests.get(HOUSE_ARCHIVE, headers={"User-Agent": UA}, verify=False)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    videos: List[Video] = []
    for div in soup.select("div.page-search-object"):
        a = div.select_one("a[href]")
        if not a:
            continue

        d = None
        title_text = a.get_text(strip=True)
        m = re.match(r"^[A-Za-z]+,\s+[A-Za-z]+\s+\d{1,2},\s+\d{4}", title_text)
        if m:
            date_str = m.group(0)
            d = datetime.strptime(date_str, "%A, %B %d, %Y").date()

        video_url = urljoin(HOUSE_ARCHIVE, a["href"])
        parsed = urlparse(video_url)
        video_param = parse_qs(parsed.query).get("video", [None])[0]
        external_id = video_param[:-4]

        videos.append(Video(
            source="house",
            external_id=external_id,
            title=external_id,
            date=d,
            url=video_url,
        ))

    cutoff = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).date()
    return [vid for vid in videos if vid.date >= cutoff]

if __name__ == "__main__":
    vids = fetch_house_videos(lookback_days=3)
    print(f"Found {len(vids)} recent House videos")
    for v in vids:
        print(v.date, v.external_id, v.title, v.url)
