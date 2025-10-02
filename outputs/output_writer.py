import os
import logging
import pandas as pd

class CSVOutput:
    @staticmethod
    async def write(df: pd.DataFrame, filename: str = "transformed_output.csv"):
        try:
            write_header = not os.path.exists(filename)
            df.to_csv(filename, mode='a', header=write_header, index=False)
            logging.info(f"Appended DataFrame to {filename}")
        except Exception as e:
            logging.error(f"Failed to append DataFrame to {filename}: {e}")

class BlockedOutput:
    @staticmethod
    async def write(df: pd.DataFrame, filename: str = "output-blocked-write.csv"):
        try:
            if os.path.exists(filename):
                if os.name == 'nt':
                    import ctypes
                    FILE_ATTRIBUTE_READONLY = 0x01
                    ctypes.windll.kernel32.SetFileAttributesW(filename, FILE_ATTRIBUTE_READONLY)
                else:
                    os.chmod(filename, 0o444)
            df.to_csv(filename, index=False)
            logging.info(f"Saved DataFrame to {filename} (should not succeed if permissions are blocked)")
        except Exception as e:
            logging.error(f"Failed to save DataFrame to blocked file {filename}: {e}")

class ConsoleOutput:
    @staticmethod
    async def write(df: pd.DataFrame):
        try:
            print("Weather DataFrame:")
            print(df)
            logging.info("Printed DataFrame to console")
        except Exception as e:
            logging.error(f"Failed to print DataFrame: {e}")