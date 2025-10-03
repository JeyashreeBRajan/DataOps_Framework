import requests
import json
from datetime import datetime

API_KEY = "e5e294b017f5da0c02a935d8a6122573"
cities = ["London,GB", "Paris,FR", "Chennai,IN","Powai,IN","Navi Mumbai,IN","Kala Ghoda,IN","Auroville,IN","Binnenstad,NL"]

def fetch_weather(city: str) -> dict:
    """Fetch weather data for a single city from OpenWeather API."""
    params = {
        "q": city,
        "appid": API_KEY,
        "units": "metric"
    }
    response = requests.get(BASE_URL, params=params)
    data = response.json()

    if response.status_code != 200 or "main" not in data:
        print(f"⚠️ Failed to fetch data for {city}: {data}")
        return None

    weather = {
        "city": data["name"],
        "country": data["sys"]["country"],
        "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "temperature": data["main"]["temp"],
        "humidity": data["main"]["humidity"],
        "pressure": data["main"]["pressure"],
        "weather": data["weather"][0]["description"],
        "wind_speed": data["wind"]["speed"]
    }
    return weather


def fetch_weather_multiple(city_list: list) -> list:
    """Fetch weather for multiple cities and return as a list of dicts.
       Also saves to JSON file (append mode).
    """
    results = []
    with open("weather_raw.json", "a") as f:  # ✅ append mode
        for city in city_list:
            record = fetch_weather(city)
            if record:
                results.append(record)
                f.write(json.dumps(record) + "\n")  # ✅ log each record
                print(f"✅ {city} -> {record['temperature']} °C")

    return results


# Debug / standalone test
if __name__ == "__main__":
    records = fetch_weather_multiple(cities)
    for r in records:
        print(r)


#Persisting data from latest run
