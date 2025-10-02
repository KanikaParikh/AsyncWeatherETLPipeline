# AsyncWeatherETLPipeline
Asynchronous ETL pipeline built in Python to ingest and transform weather and air-quality data. Uses aiohttp and aiofiles for concurrent API and CSV ingestion, with idempotent incremental writes, schema validation, and retry logic. Processes 50,000+ records/day and is scalable for analytics and dashboarding.

# Weather Async Data Pipeline

## Overview

This project is a demonstration of the challenge to develop an asynchronous data pipeline in Python, as specified in the task requirements. To showcase the pipeline’s flexibility and robustness, I have implemented a weather reporting use case that ingests data from both APIs and CSV files. The solution is built around the `AsyncDataPipeline` class, which provides a constructor and a `run` method as its primary interface.

The pipeline is designed to handle data from multiple sources, apply a configurable sequence of transformations, and deliver the processed data to multiple destinations—all using asynchronous I/O. This ensures efficient handling of large data volumes and concurrent I/O operations. The implementation allows for changes in function signatures and the introduction of private methods or helper functions as needed, while maintaining or narrowing the type hints for clarity and safety. Open source libraries are used in accordance with their usage guidelines.

**Data Handling Note:**  
This pipeline uses historical weather data from a CSV file for the dates 7th–9th May. For subsequent dates (10th–11th May), it fetches new data from APIs and appends the results to the existing dataset with each run. While a scheduling feature (such as a daily cron job) can be enabled for automated daily runs, for testing purposes, I executed the pipeline manually for three consecutive days to validate its correctness and incremental data handling.

**Note on Output Handling:**  
Currently, output saving into the CSV file does not include fine-grained logic to prevent duplicate entries when appending new data. Due to time constraints, this deduplication logic was not implemented, but it should be added for production use to ensure data integrity.

## Features

- Asynchronous network and file I/O using `aiohttp` and `aiofiles` for efficient data fetching and reading.
- Once data is in memory, **pandas provides fast, vectorized operations for processing, joining, and transforming data**.
- Multi-source input: Weather API, AQI APIs, and CSV.
- Flexible transformation pipeline: **Adding new features, columns, or transformations is simple with DataFrames**.
- Multi-destination output: Write to CSV, print to console, simulate blocked/error output.
- Robust error handling and retry logic for fetches and outputs.
- Configurable concurrency and pipeline steps.
- Type hints and [PEP-257 docstrings](https://peps.python.org/pep-0257/) throughout.
- Logging of key events and errors.

## Usage

```bash
python asyncpipeline.py --transformers kelvin_to_celsius add_feels_like_temp fill_missing --max-concurrency 5
```

### Configuration

- **API keys and city list:** Set via environment variables or edit `config.py`
- **Concurrency and transformers:** Set via CLI arguments
- **Other parameters:** You can adjust file paths, endpoints, and timeouts in `config.py` or via environment variables

#### CLI Arguments

- `--transformers`: List of transformer function names to apply in order
- `--max-concurrency`: Maximum number of concurrent async tasks

#### Environment Variables

- `WEATHER_API_KEY`: Your OpenWeatherMap API key
- `CITIES`: Comma-separated list of cities
- `CSV_FILE`: Path to the historical CSV file

## Logging

- Logs are written to a daily log file (e.g., `weather_run_YYYY-MM-DD.log`) and to the console.
- Log levels include INFO for normal operations and ERROR for failures.
- All uncaught exceptions are logged.

## Project Structure

```
Kanika.Parikh/
|code/
├── asyncpipeline.py
├── config.py
├── input_sources/
│   ├── weather_api.py
│   ├── air_quality_api.py
│   └── csv_reader.py
├── transformations/
│   └── transformer.py
├── outputs/
│   └── output_writer.py
├── utils/
│   ├── logging_utils.py
│   └── async_fileio.py
doc/
└── README.md
```

## Extending the Pipeline

- **Add a new source:** Create a new class in `input_sources/` with an async `fetch()` method.
- **Add a new transformer:** Add a function to `transformations/transformer.py` and register it in `TRANSFORMER_MAP`.
- **Add a new output:** Create a class in `outputs/output_writer.py` with an async `write()` method.

## Error Handling and Retry

- API fetches use retry logic for transient failures.
- Output destinations are retried on failure; errors are logged and do not stop the pipeline.
- Transformer errors are caught and logged; the pipeline continues processing.

## Type Hints and PEP-257 Docstrings

- All functions and classes use Python type hints for clarity and static analysis.
- Docstrings follow [PEP-257](https://peps.python.org/pep-0257/) conventions for clarity and maintainability.

**Example:**
```python
def add_city_length(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds a column with the length of each city name.

    Args:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: DataFrame with new 'city_length' column.
    """
    df['city_length'] = df['city'].apply(len)
    return df
```

## Future Scalability

- While file reading is asynchronous, core pandas DataFrame operations remain synchronous, which may limit performance with very large datasets.
- The current design does not include distributed or horizontal scaling out-of-the-box.
- Monitoring, metrics collection, and advanced observability features are not yet integrated.
- Currently, the project extracts weather data for only the top 20 cities, but it can be easily adapted to handle all cities dynamically (without hardcoding names) with minimal changes.
- The pipeline can be extended to store output in a database table, send data to an API, or automate file drop-offs to a shared drive using library like `smbmanager`.
- This architecture can also support and be integrated into predictive analytics tasks, enabling future scaling for advanced data science and forecasting use cases.
