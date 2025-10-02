import pandas as pd
import numpy as np

def kelvin_to_celsius(df: pd.DataFrame) -> pd.DataFrame:
    """Converts temperature from Kelvin to Celsius."""
    df['temp_celsius'] = df['temp_k'].apply(lambda x: round(x - 273.15, 2) if pd.notnull(x) else "NA")
    return df

def add_feels_like_temp(df: pd.DataFrame) -> pd.DataFrame:
    """Adds a 'feels_like_temp' column in Celsius."""
    def compute_feels_like(row):
        try:
            if 'feels_like' in row and pd.notnull(row['feels_like']):
                return round(row['feels_like'] - 273.15, 2)
            temp_c = float(row['temp_celsius']) if row['temp_celsius'] != "NA" else None
            humidity = float(row['humidity']) if row['humidity'] != "NA" else None
            wind = float(row['wind_speed']) if row['wind_speed'] != "NA" else None
            if temp_c is None or humidity is None or wind is None:
                return "NA"
            # Simplified heat index formula
            if temp_c >= 27 and humidity >= 40:
                hi = (0.5 * temp_c) + (0.5 * humidity) - 10
                return round(hi, 2)
            # Simplified wind chill formula
            if temp_c <= 10 and wind > 1.3:
                wc = temp_c - (0.7 * wind)
                return round(wc, 2)
            return temp_c
        except Exception:
            return "NA"
    df['feels_like_temp'] = df.apply(compute_feels_like, axis=1)
    return df

def add_humidity_level(df: pd.DataFrame) -> pd.DataFrame:
    """Adds a 'humidity_level' column: 'low' (<40), 'moderate' (40-70), 'high' (>70)."""
    def level(h):
        try:
            h = float(h)
            if h < 40:
                return "low"
            elif h <= 70:
                return "moderate"
            else:
                return "high"
        except:
            return "NA"
    df['humidity_level'] = df['humidity'].apply(level)
    return df

def add_weather_score(df: pd.DataFrame) -> pd.DataFrame:
    """Adds a 'weather_score' column for event planning."""
    def score(row):
        try:
            temp = float(row['temp_celsius']) if row['temp_celsius'] != "NA" else 20
            humidity = float(row['humidity']) if row['humidity'] != "NA" else 50
            wind = float(row['wind_speed']) if row['wind_speed'] != "NA" else 2
            desc = str(row['description']).lower()
            s = 10
            if any(word in desc for word in ['rain', 'drizzle', 'storm', 'shower', 'downpour', 'sprinkle']):
                s -= 4
            if humidity > 80:
                s -= 2
            elif humidity < 30:
                s -= 1
            if wind > 8:
                s -= 2
            if temp < 10 or temp > 32:
                s -= 2
            return max(0, min(10, s))
        except:
            return "NA"
    df['weather_score'] = df.apply(score, axis=1)
    return df

def add_is_rainy(df: pd.DataFrame) -> pd.DataFrame:
    """Sets 'is_rainy' True if description contains rain, drizzle, shower, or similar."""
    rain_words = ['rain', 'drizzle', 'shower', 'storm', 'downpour', 'sprinkle']
    df['is_rainy'] = df['description'].apply(
        lambda x: any(word in str(x).lower() for word in rain_words) if pd.notnull(x) else False
    )
    return df

def clean_description(df: pd.DataFrame) -> pd.DataFrame:
    """Cleans and capitalizes the weather description."""
    df['description'] = df['description'].fillna("NA").str.capitalize()
    return df

def fill_missing(df: pd.DataFrame) -> pd.DataFrame:
    """Fills missing values: uses np.nan for numerics, 'NA' for strings."""
    string_cols = ['source', 'timestamp', 'aqi', 'aqi_open_meteo', 'description']
    numeric_cols = ['humidity', 'wind_speed', 'temp_k', 'feels_like', 'temp_celsius', 'feels_like_temp', 'weather_score']

    for col in string_cols:
        if col in df.columns:
            df[col] = df[col].astype('object')
            df[col] = df[col].fillna("NA")
    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].fillna(np.nan)
    return df

class TransformerPipeline:
    def __init__(self, transformers):
        self.transformers = transformers

    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        for transformer in self.transformers:
            df = transformer(df)
        columns = [col for col in [
            'city', 'temp_celsius', 'feels_like_temp', 'humidity', 'humidity_level',
            'wind_speed', 'is_rainy', 'weather_score', 'aqi', 'aqi_open_meteo', 'description', 'timestamp', 'source'
        ] if col in df.columns]
        return df[columns]