import os

API_KEY = os.getenv("WEATHER_API_KEY", "924fbf7b7da9fb8bb8763303b4e0b122")
CITIES = os.getenv("CITIES", "London,New York,Mumbai,Toronto,Tokyo,Paris,Sydney,Cape Town,São Paulo,Moscow,Beijing,Seoul,Dubai,Bangkok,Mexico City,Istanbul,Berlin,Singapore,Los Angeles,Rome").split(",")
MAX_CONCURRENCY = int(os.getenv("MAX_CONCURRENCY", 5))
CSV_FILE = os.getenv("CSV_FILE", "historical_weather_data.csv")