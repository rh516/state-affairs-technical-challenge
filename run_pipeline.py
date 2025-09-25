from downloader import download_concurrent, download_one
from persistence import connect, init_db, upsert_videos, fetch_all
from scrapers.house_scraper import fetch_videos
from transcriber import transcribe_videos

def main():
    conn = connect()
    init_db(conn)

    # House
    house_videos = fetch_videos(lookback_days=14)
    new_house_vids = upsert_videos(conn, house_videos)
    print(f"House: {new_house_vids} new videos")

    # conn.execute("DELETE FROM videos;")  # removes all rows
    # conn.commit()

    # print("Current rows in DB:", fetch_all(conn))

    download_successes, download_failures = download_concurrent(conn)
    if download_successes or download_failures:
        print(f"Downloaded {download_successes} videos with {download_failures} failures")

    transcribe_successes, transcribe_failures = transcribe_videos(conn)
    if transcribe_successes or transcribe_failures:
        print(f"Transcribed {transcribe_successes} videos with {transcribe_failures} failures")

if __name__ == "__main__":
    main()