import asyncio
import aiohttp
import logging
import argparse
import pandas as pd
pd.set_option('future.no_silent_downcasting', True)

from config import CITIES, MAX_CONCURRENCY
from utils.logging_utils import setup_daily_log
from input_sources.weather_api import WeatherAPIInput
from input_sources.air_quality_api import OpenAQInput, OpenMeteoInput
from input_sources.csv_reader import CSVInput
from transformations.transformer import TransformerPipeline
from outputs.output_writer import CSVOutput, BlockedOutput, ConsoleOutput
from utils.async_fileio import async_read_csv, async_write_csv

from transformations.transformer import (
    kelvin_to_celsius, add_feels_like_temp, add_humidity_level,
    add_weather_score, add_is_rainy, clean_description, fill_missing
)

TRANSFORMER_MAP = {
    "kelvin_to_celsius": kelvin_to_celsius,
    "add_feels_like_temp": add_feels_like_temp,
    "add_humidity_level": add_humidity_level,
    "add_weather_score": add_weather_score,
    "add_is_rainy": add_is_rainy,
    "clean_description": clean_description,
    "fill_missing": fill_missing
}

class AsyncDataPipeline:
    def __init__(
        self,
        sources,
        transformers=None,
        destinations=None,
        max_concurrent_tasks=5
    ):
        """
        :param sources: List of callables that asynchronously collect data.
        :param transformers: List of callables that transform or process data.
        :param destinations: List of callables that asynchronously send data.
        :param max_concurrent_tasks: Maximum number of concurrent async tasks allowed.
        """
        self.sources = sources
        self.transformers = transformers if transformers else []
        self.destinations = destinations if destinations else []
        self.max_concurrent_tasks = max_concurrent_tasks

    async def run(self):
        setup_daily_log()
        semaphore = asyncio.Semaphore(self.max_concurrent_tasks)
        async with aiohttp.ClientSession() as session:
            async def sem_task(coro):
                async with semaphore:
                    return await coro

            # Collect data from all sources
            api_tasks = [sem_task(WeatherAPIInput.fetch(session, city)) for city in CITIES]
            aqi_tasks = [sem_task(OpenAQInput.fetch(session, city)) for city in CITIES]
            aqi_meteo_tasks = [sem_task(OpenMeteoInput.fetch(session, city)) for city in CITIES]
            all_results = await asyncio.gather(*(api_tasks + aqi_tasks + aqi_meteo_tasks), return_exceptions=True)

        # Separate results
        weather_results, aqi_results, aqi_meteo_results = [], [], []
        for result in all_results:
            if isinstance(result, Exception):
                logging.error(f"Fetch failed after retries: {result}")
            elif "aqi_open_meteo" in result:
                aqi_meteo_results.append(result)
            elif "aqi" in result:
                aqi_results.append(result)
            else:
                weather_results.append(result)

        df_weather = pd.DataFrame(weather_results)
        df_aqi = pd.DataFrame(aqi_results)
        df_aqi_meteo = pd.DataFrame(aqi_meteo_results)

        # Merge AQI into weather DataFrame on city
        if not df_aqi.empty:
            df_weather = pd.merge(df_weather, df_aqi[['city', 'aqi']], on='city', how='left')
        # Merge AQI Open-Meteo on city and date (from timestamp)
        if not df_aqi_meteo.empty and not df_weather.empty:
            df_weather['date'] = pd.to_datetime(df_weather['timestamp']).dt.date.astype(str)
            df_aqi_meteo['date'] = pd.to_datetime(df_aqi_meteo['timestamp']).dt.date.astype(str)
            df_weather = pd.merge(
                df_weather,
                df_aqi_meteo[['city', 'date', 'aqi_open_meteo']],
                on=['city', 'date'],
                how='left'
            )
            df_weather.drop(columns=['date'], inplace=True)

        # Async read all CSV records and append to the weather DataFrame
        try:
            csv_df = await async_read_csv("historical_weather_data.csv")
        except Exception as e:
            logging.error(f"Async CSV read failed: {e}")
            csv_df = pd.DataFrame()
        if not csv_df.empty:
            df_weather = pd.concat([df_weather, csv_df], ignore_index=True)
        df_weather.sort_values(by=['city', 'timestamp'], ascending=[True, True], inplace=True)

        # Apply transformations with error handling
        for transformer in self.transformers:
            df_weather = safe_transform(transformer, df_weather)

        # Dispatch to outputs with retry logic
        await asyncio.gather(
            *(retry_output(dest, df_weather) for dest in self.destinations)
        )

def safe_transform(transformer, df):
    try:
        return transformer(df)
    except Exception as e:
        logging.error(f"Transformer {getattr(transformer, '__name__', str(transformer))} failed: {e}")
        return df

async def retry_output(dest, df, retries=3, delay=2):
    for attempt in range(1, retries + 1):
        try:
            await dest.write(df)
            return
        except Exception as e:
            logging.error(f"Output {getattr(dest, '__name__', str(dest))} failed (attempt {attempt}): {e}")
            await asyncio.sleep(delay)
    logging.error(f"All retries failed for output {getattr(dest, '__name__', str(dest))}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Weather Data Pipeline")
    parser.add_argument(
        "--transformers",
        nargs="+",
        default=[
            "kelvin_to_celsius",
            "add_feels_like_temp",
            "add_humidity_level",
            "add_weather_score",
            "add_is_rainy",
            "clean_description",
            "fill_missing"
        ],
        help="List of transformer function names to apply in order"
    )
    parser.add_argument(
        "--max-concurrency",
        type=int,
        default=MAX_CONCURRENCY,
        help="Maximum number of concurrent tasks"
    )
    args = parser.parse_args()
    transformers = [TRANSFORMER_MAP[name] for name in args.transformers if name in TRANSFORMER_MAP]
    destinations = [CSVOutput, ConsoleOutput, BlockedOutput]
    pipeline = AsyncDataPipeline(
        sources=[],  # Sources are handled inside the run method for this use case
        transformers=transformers,
        destinations=destinations,
        max_concurrent_tasks=args.max_concurrency
    )
    asyncio.run(pipeline.run())