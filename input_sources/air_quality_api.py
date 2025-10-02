import aiohttp
from typing import Dict, Any
from datetime import datetime
import logging
from utils.logging_utils import async_retry

class OpenAQInput:
    @staticmethod
    @async_retry(retries=3, delay=2)
    async def fetch(session: aiohttp.ClientSession, city: str) -> Dict[str, Any]:
        url = f"https://api.openaq.org/v2/latest?city={city}"
        try:
            logging.info(f"Fetching air quality for {city} from OpenAQ")
            async with session.get(url, ssl=True) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    aqi = "NA"
                    if data.get("results"):
                        for measurement in data["results"][0].get("measurements", []):
                            if measurement["parameter"] in ["pm25", "pm10"]:
                                aqi = measurement["value"]
                                break
                    return {
                        "city": city,
                        "aqi": aqi,
                        "source": "openaq",
                        "timestamp": datetime.utcnow().isoformat()
                    }
                else:
                    logging.error(f"OpenAQ API error for {city}: Status {resp.status}")
                    return {"city": city, "aqi": "NA", "source": "openaq", "timestamp": None}
        except Exception as e:
            logging.error(f"Exception fetching OpenAQ data for {city}: {e}")
            return {"city": city, "aqi": "NA", "source": "openaq", "timestamp": None}

class OpenMeteoInput:
    @staticmethod
    @async_retry(retries=3, delay=2)
    async def fetch(session: aiohttp.ClientSession, city: str) -> Dict[str, Any]:
        city_coords = {
            "London": (51.5074, -0.1278),
            "New York": (40.7128, -74.0060),
            "Mumbai": (19.0760, 72.8777),
            "Tokyo": (35.6895, 139.6917),
            "Toronto": (43.651070, -79.347015),
            "Sydney": (-33.8688, 151.2093),
            "Paris": (48.8566, 2.3522),
            "Beijing": (39.9042, 116.4074),
            "Moscow": (55.7558, 37.6173),
            "Los Angeles": (34.0522, -118.2437),
            "Chicago": (41.8781, -87.6298),
            "Singapore": (1.3521, 103.8198),
            "Dubai": (25.2048, 55.2708),
            "Johannesburg": (-26.2041, 28.0473),
            "São Paulo": (-23.5505, -46.6333),
            "Mexico City": (19.4326, -99.1332),
            "Istanbul": (41.0082, 28.9784),
            "Seoul": (37.5665, 126.9780),
            "Berlin": (52.5200, 13.4050),
            "Hong Kong": (22.3193, 114.1694)
        }
        lat, lon = city_coords.get(city, (None, None))
        if lat is None or lon is None:
            logging.error(f"No coordinates found for city: {city}")
            return {"city": city, "aqi_open_meteo": "NA", "source": "open-meteo", "timestamp": None}
        url = f"https://air-quality-api.open-meteo.com/v1/air-quality?latitude={lat}&longitude={lon}&hourly=pm2_5"
        try:
            logging.info(f"Fetching air quality for {city} from Open-Meteo")
            async with session.get(url, ssl=True) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    pm25 = "NA"
                    if "hourly" in data and "pm2_5" in data["hourly"]:
                        pm25_list = data["hourly"]["pm2_5"]
                        if isinstance(pm25_list, list) and len(pm25_list) > 0:
                            pm25 = pm25_list[-1]
                    return {
                        "city": city,
                        "aqi_open_meteo": pm25,
                        "source": "open-meteo",
                        "timestamp": datetime.utcnow().isoformat()
                    }
                else:
                    logging.error(f"Open-Meteo API error for {city}: Status {resp.status}")
                    return {"city": city, "aqi_open_meteo": "NA", "source": "open-meteo", "timestamp": None}
        except Exception as e:
            logging.error(f"Exception fetching Open-Meteo data for {city}: {e}")
            return {"city": city, "aqi_open_meteo": "NA", "source": "open-meteo", "timestamp": None}