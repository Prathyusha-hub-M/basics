import requests

API_URL = "https://datacatalog.cookcountyil.gov/resource/y282-6ig3.json"

params = {
    "$limit": 5,
    "$where": "year > 2024"
}

r = requests.get(API_URL, params=params, timeout=60)
r.raise_for_status()
data = r.json()

print("Rows from API:", len(data))
for row in data:
    print(int(float(row.get("year"))), row.get("row_id"))