import aiohttp
from typing import List, Dict, Any
from datetime import datetime
import logging
from config import API_KEY
from utils.logging_utils import async_retry

class WeatherAPIInput:
    @staticmethod
    @async_retry(retries=3, delay=2)
    async def fetch(session: aiohttp.ClientSession, city: str) -> Dict[str, Any]:
        url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}"
        try:
            logging.info(f"Fetching weather for {city} from API")
            async with session.get(url, ssl=True) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return {
                        "city": city,
                        "temp_k": data['main'].get('temp', None),
                        "humidity": data['main'].get('humidity', None),
                        "wind_speed": data['wind'].get('speed', None),
                        "description": data['weather'][0].get('description', None) if data.get('weather') and len(data['weather']) > 0 else None,
                        "feels_like": data['main'].get('feels_like', None),
                        "source": "api",
                        "timestamp": datetime.utcnow().isoformat()
                    }
                else:
                    logging.error(f"API error for {city}: Status {resp.status}")
                    return {"city": city, "temp_k": None, "humidity": None, "wind_speed": None, "description": None, "feels_like": None, "source": "api", "timestamp": None}
        except Exception as e:
            logging.error(f"Exception fetching API data for {city}: {e}")
            raise