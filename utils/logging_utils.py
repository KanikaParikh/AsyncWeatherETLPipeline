import logging
from datetime import datetime
import sys

def setup_daily_log():
    transaction_date = datetime.now().strftime("%Y-%m-%d")
    log_filename = f"weather_run_{transaction_date}.log"
    file_handler = logging.FileHandler(log_filename, mode='a', encoding='utf-8')
    formatter = logging.Formatter("%(asctime)s %(levelname)s: %(message)s")
    file_handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    # Remove all handlers before adding (avoid duplicate logs)
    if root_logger.hasHandlers():
        root_logger.handlers.clear()
    root_logger.addHandler(file_handler)
    # Set level to INFO to capture all steps, not just errors
    root_logger.setLevel(logging.INFO)
    def handle_exception(exc_type, exc_value, exc_traceback):
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return
        logging.error("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))
    sys.excepthook = handle_exception

def async_retry(retries=3, delay=2):
    def decorator(func):
        async def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(1, retries + 1):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    last_exc = e
                    logging.warning(f"Retry {attempt}/{retries} for {func.__name__} due to error: {e}")
                    await asyncio.sleep(delay)
            logging.error(f"All retries failed for {func.__name__}: {last_exc}")
            raise last_exc
        return wrapper
    return decorator