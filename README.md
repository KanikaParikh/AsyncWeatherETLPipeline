# AsyncWeatherETLPipeline
Asynchronous ETL pipeline built in Python to ingest and transform weather and air-quality data. Uses aiohttp and aiofiles for concurrent API and CSV ingestion, with idempotent incremental writes, schema validation, and retry logic. Processes 50,000+ records/day and is scalable for analytics and dashboarding.
