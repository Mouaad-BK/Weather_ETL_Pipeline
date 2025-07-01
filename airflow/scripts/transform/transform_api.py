import pandas as pd

# Desired column order in final DataFrame
FINAL_COLS = [
    "Ville","Date","Heure",
    "Température (°C)","Ressentie (°C)",
    "Température max (°C)","Température min (°C)",
    "Humidité (%)","Pression (hPa)","Nuages (%)",
    "Visibilité (m)","Vent (km/h)","Direction du vent (°)",
    "Lever du soleil","Coucher du soleil",
    "Neige (1h mm)","Pluie (1h mm)","Description"
]

def transform_api_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    - Shift timestamps to GMT+1.
    - Convert wind speed from m/s to km/h.
    - Shift sunrise/sunset by +1 hour and keep only HH:MM:SS.
    """
    df = df.copy()
    
    # Combine Date+Heure, then add 1 hour
    dt = pd.to_datetime(df["Date"] + " " + df["Heure"]) + pd.Timedelta(hours=1)
    df["Date"]  = dt.dt.strftime("%Y-%m-%d")
    df["Heure"] = dt.dt.strftime("%H:%M:%S")

    # Convert wind
    df["Vent (km/h)"] = (df["Vent (m/s)"] * 3.6).round(2)
    df.drop(columns=["Vent (m/s)"], inplace=True)

    # Shift Lever/Coucher du soleil by +1h and keep only time
    df["Lever du soleil"] = (
        pd.to_datetime(df["Lever du soleil"], errors="coerce") + pd.Timedelta(hours=1)
    ).dt.strftime("%H:%M:%S")
    
    df["Coucher du soleil"] = (
        pd.to_datetime(df["Coucher du soleil"], errors="coerce") + pd.Timedelta(hours=1)
    ).dt.strftime("%H:%M:%S")

    # Fill missing
    df.fillna(0.0, inplace=True)

    # Reorder
    return df.reindex(columns=FINAL_COLS)

def build_api_df() -> pd.DataFrame:
    # Run extract → transform and return final API DataFrame.
    from scripts.extract.extract_api import extract_weather_data, API_KEY, CITIES
    raw = extract_weather_data(API_KEY, CITIES)
    return transform_api_df(raw)
