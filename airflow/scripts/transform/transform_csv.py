import os
import sys
import pandas as pd

# Add project root to PYTHONPATH
current_dir = os.path.dirname(__file__)
project_root = os.path.abspath(os.path.join(current_dir, ".."))
sys.path.append(project_root)

from extract.extract_csv import extract_csv_data

# Unit conversion helpers
def f_to_c(f):       return (f - 32) * 5.0 / 9.0
def miles_to_m(m):   return m * 1609.34
def mph_to_kmh(mph): return mph * 1.60934
def inches_to_mm(inch): return inch * 25.4  # ✅ conversion pluie/neige

# Description mapping EN → FR
DESC_MAP = {
    "clear": "Dégagé",
    "partly cloudy": "Partiellement nuageux",
    "partially cloudy": "Partiellement nuageux",
    "overcast": "Couvert",
    "rain": "Pluie",
    "light rain": "Pluie légère",
    "heavy rain": "Forte pluie",
    "snow": "Neige",
    "light snow": "Neige légère",
    "thunderstorm": "Orage",
    "fog": "Brouillard",
    "haze": "Brume",
    "scattered clouds": "Nuages épars"
}

# Column order
FINAL_COLS = [
    "Ville", "Date", "Heure",
    "Température (°C)", "Ressentie (°C)",
    "Température max (°C)", "Température min (°C)",
    "Humidité (%)", "Pression (hPa)", "Nuages (%)",
    "Visibilité (m)", "Vent (km/h)", "Direction du vent (°)",
    "Lever du soleil", "Coucher du soleil",
    "Neige (1h mm)", "Pluie (1h mm)", "Description"
]

# City name correction
CITY_FIX = {
    "guercif": "Guercif", "gurcif": "Guercif", "guerçif": "Guercif",
    "nador": "Nador", "oujda": "Oujda", "jerada": "Jerada",
    "driouch": "Driouch", "berkane": "Berkane", "berkan": "Berkane",
    "figuig": "Figuig", "taourirt": "Taourirt"
}

def translate_description(desc: str) -> str:
    if pd.isna(desc) or not isinstance(desc, str):
        return desc
    parts = [part.strip().lower() for part in desc.split(",")]
    translated = [DESC_MAP.get(p, p) for p in parts if p]
    return ", ".join(translated).capitalize()

def transform_csv_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    if "Ville" in df.columns:
        df["Ville"] = (
            df["Ville"].astype(str).str.strip().str.lower()
            .map(CITY_FIX).fillna("Autre")
        )

    if "Température (°F)" in df.columns:
        df["Température (°C)"] = df["Température (°F)"].apply(f_to_c).round(2)
        df["Ressentie (°C)"] = df["Ressentie (°F)"].apply(f_to_c).round(2)

    if "Visibilité (miles)" in df.columns:
        df["Visibilité (m)"] = df["Visibilité (miles)"].apply(miles_to_m).round(2)

    if "Vent (miles/h)" in df.columns:
        df["Vent (km/h)"] = df["Vent (miles/h)"].apply(mph_to_kmh).round(2)

    # ✅ Conversion pluie et neige
    if "Pluie (1h mm)" in df.columns:
        df["Pluie (1h mm)"] = df["Pluie (1h mm)"].apply(inches_to_mm).round(2)
    if "Neige (1h mm)" in df.columns:
        df["Neige (1h mm)"] = df["Neige (1h mm)"].apply(inches_to_mm).round(2)

    if "Date" in df.columns:
        dt = pd.to_datetime(df["Date"], errors="coerce")
        df["Date"] = dt.dt.strftime("%Y-%m-%d")
        df["Heure"] = dt.dt.strftime("%H:%M:%S")

    for col in ("Température max (°C)", "Température min (°C)", "Pression (hPa)"):
        if col not in df.columns:
            df[col] = 0.0

    for col in ("Lever du soleil", "Coucher du soleil"):
        if col not in df.columns:
            df[col] = pd.NA

    for old in ("Température (°F)", "Ressentie (°F)", "Visibilité (miles)", "Vent (miles/h)"):
        if old in df.columns:
            df.drop(columns=[old], inplace=True)

    if {"Ville", "Date", "Heure"}.issubset(df.columns):
        df.drop_duplicates(subset=["Ville", "Date", "Heure"], inplace=True)

    if "Description" in df.columns:
        df["Description"] = df["Description"].apply(translate_description)

    num_cols = df.select_dtypes(include="number").columns
    df[num_cols] = df[num_cols].fillna(0.0)

    return df.reindex(columns=FINAL_COLS)

def build_global_csv_df() -> pd.DataFrame:
    city_dfs = extract_csv_data()
    dfs = [transform_csv_df(df) for df in city_dfs.values() if not df.empty]
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame(columns=FINAL_COLS)
