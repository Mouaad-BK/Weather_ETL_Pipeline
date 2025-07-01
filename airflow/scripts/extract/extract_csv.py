import os
import pandas as pd

# Folder where city subfolders with CSV files are located
BASE_PATH = "data_csv"

# List of cities to process
CITIES = ["nador", "oujda", "berkane", "driouch", "jerada", "figuig", "guercif", "taourirt"]

# Mapping from original column names to standardized French names (with units)
COLUMN_MAPPING = {
    "name": "Ville",
    "datetime": "Date",
    "temp": "Température (°F)",
    "feelslike": "Ressentie (°F)",
    "humidity": "Humidité (%)",
    "sealevelpres": "Pression (hPa)",
    "cloudcover": "Nuages (%)",
    "visibility": "Visibilité (miles)",
    "windspeed": "Vent (miles/h)",
    "winddir": "Direction du vent (°)",
    "precip": "Pluie (1h mm)",
    "snow": "Neige (1h mm)",
    "conditions": "Description"
}

def extract_csv_data(base_path: str = BASE_PATH) -> dict[str, pd.DataFrame]:
    """
    Load historical weather CSVs for each city and return a dictionary of DataFrames.
    Each DataFrame is cleaned and columns are renamed using COLUMN_MAPPING.
    """
    
    city_dfs: dict[str, pd.DataFrame] = {}

    for city in CITIES:
        # Construct path to the city's folder
        city_dir = os.path.join(base_path, city)
        if not os.path.isdir(city_dir):
            continue  # Skip city if folder doesn't exist

        frames = []

        # Process all .csv files in this city's folder
        for fname in sorted(os.listdir(city_dir)):
            if not fname.endswith(".csv"):
                continue

            # Load CSV
            df = pd.read_csv(os.path.join(city_dir, fname))

            # Keep only known/expected columns
            keep = [c for c in COLUMN_MAPPING if c in df.columns]
            if not keep:
                continue

            # Rename columns to standardized names
            df = df[keep].rename(columns={c: COLUMN_MAPPING[c] for c in keep})
            frames.append(df)

        # Combine all monthly CSVs into one DataFrame for the city
        city_dfs[city] = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    # Return a dict: {city: DataFrame}
    return city_dfs
