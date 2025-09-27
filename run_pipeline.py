from downloader import download_concurrent
from persistence import connect, init_db, upsert_videos
from scrapers.house_scraper import fetch_house_videos
from scrapers.senate_scraper import fetch_senate_videos
from transcriber import transcribe_videos


def main():
    conn = connect()
    init_db(conn)

    house_videos = fetch_house_videos(lookback_days=3)
    new_house_vids = upsert_videos(conn, house_videos)
    print(f"House: {new_house_vids} new videos")

    senate_videos = fetch_senate_videos(lookback_days=7)
    new_senate_vids = upsert_videos(conn, senate_videos)
    print(f"Senate: {new_senate_vids} new videos")

    download_successes, download_failures = download_concurrent(conn)
    if download_successes or download_failures:
        print(f"Downloaded {download_successes} videos with {download_failures} failures")

    transcribe_successes, transcribe_failures = transcribe_videos(conn)
    if transcribe_successes or transcribe_failures:
        print(f"Transcribed {transcribe_successes} videos with {transcribe_failures} failures")


if __name__ == "__main__":
    main()
