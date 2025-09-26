import requests
import json

SENATE_URL = "https://2kbyogxrg4.execute-api.us-west-2.amazonaws.com/61b3adc8124d7d000891ca5c/home/recent"
HEADERS = {
    "User-Agent": "StateAffairsIngest/1.0 (+rrhuang99@gmail.com)"
}

def fetch_videos():
    response = requests.get(SENATE_URL, headers=HEADERS, timeout=30)
    response.raise_for_status()

    data = response.json()

    print(json.dumps(data, indent=4))

if __name__ == "__main__":
    fetch_videos()