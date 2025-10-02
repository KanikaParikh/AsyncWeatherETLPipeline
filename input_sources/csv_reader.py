import pandas as pd
import logging
from config import CSV_FILE

class CSVInput:
    @staticmethod
    def fetch() -> pd.DataFrame:
        try:
            df = pd.read_csv(CSV_FILE)
            if 'source' not in df.columns:
                df['source'] = 'csv'
            return df
        except Exception as e:
            logging.error(f"Error reading all CSV records: {e}")
            return pd.DataFrame()