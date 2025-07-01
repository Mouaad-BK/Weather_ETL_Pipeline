import os
import requests
import pandas as pd
from datetime import datetime, timezone
from dotenv import load_dotenv, find_dotenv

# Load API key from .env
load_dotenv(find_dotenv())
API_KEY = os.getenv("OPENWEATHER_API_KEY")

# Target cities
CITIES = ["Driouch", "Nador", "Berkane", "Oujda", "Taourirt", "Guercif", "Jerada", "Figuig"]

def extract_weather_data(api_key: str, cities: list[str]) -> pd.DataFrame:
    """
    Fetch current weather for each city.
    Raises if API key is missing.
    """
    if not api_key:
        raise ValueError("Missing OPENWEATHER_API_KEY")
    url = "http://api.openweathermap.org/data/2.5/weather"
    records = []
    for city in cities:
        # Use lat/lon for Figuig, name lookup otherwise
        params = (
            {"lat": 32.1097, "lon": -1.2297, "appid": api_key, "units": "metric", "lang": "fr"}
            if city.lower() == "figuig"
            else {"q": city, "appid": api_key, "units": "metric", "lang": "fr"}
        )
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        now = datetime.utcnow().replace(tzinfo=timezone.utc)
        # Build a record for this city
        records.append({
            "Ville": city,
            "Date": now.strftime("%Y-%m-%d"),
            "Heure": now.strftime("%H:%M:%S"),
            "Température (°C)": data["main"]["temp"],
            "Ressentie (°C)": data["main"]["feels_like"],
            "Température max (°C)": data["main"]["temp_max"],
            "Température min (°C)": data["main"]["temp_min"],
            "Humidité (%)": data["main"]["humidity"],
            "Pression (hPa)": data["main"]["pressure"],
            "Nuages (%)": data["clouds"]["all"],
            "Visibilité (m)": data.get("visibility", 0.0),
            "Vent (m/s)": data["wind"]["speed"],
            "Direction du vent (°)": data["wind"].get("deg", 0.0),
            "Lever du soleil": datetime.fromtimestamp(data["sys"]["sunrise"], tz=timezone.utc)
                                .strftime("%H:%M:%S"),
            "Coucher du soleil": datetime.fromtimestamp(data["sys"]["sunset"], tz=timezone.utc)
                                .strftime("%H:%M:%S"),
            "Neige (1h mm)": data.get("snow", {}).get("1h", 0.0),
            "Pluie (1h mm)": data.get("rain", {}).get("1h", 0.0),
            "Description": data["weather"][0]["description"] or ""
        })
    return pd.DataFrame(records)
