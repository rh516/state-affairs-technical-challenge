from persistence import connect, init_db, upsert_videos, list_all
from scrapers.house_scraper import fetch_videos

def main():
    conn = connect()
    init_db(conn)

    # House
    house_videos = fetch_videos(lookback_days=60)
    new_house = upsert_videos(conn, house_videos)
    print(f"House: {new_house} new videos")
    print("Current rows in DB:")
    list_all(conn)

if __name__ == "__main__":
    main()