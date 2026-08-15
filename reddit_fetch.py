import requests
import json

URL = "https://www.reddit.com/r/all/new.json?limit=100"

headers = {
    "User-Agent": "ubuntu:simple_dump:v1.0"
}

response = requests.get(URL, headers=headers, timeout=30)

if response.status_code != 200:
    raise Exception(f"Reddit API error: {response.status_code}")

data = response.json()

posts = data["data"]["children"]

with open("reddit_raw_data.jsonl", "w", encoding="utf-8") as file:

    for post in posts:
        record = post["data"]
        file.write(json.dumps(record) + "\n")

print(f"Fetched {len(posts)} Reddit posts")
