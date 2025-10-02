import aiofiles
import pandas as pd
from io import StringIO

async def async_read_csv(filepath: str) -> pd.DataFrame:
    async with aiofiles.open(filepath, mode='r') as f:
        content = await f.read()
    df = pd.read_csv(StringIO(content))
    # Convert all columns to object dtype to allow safe filling with "NA"
    df = df.astype('object')
    df.fillna("NA", inplace=True)
    return df

async def async_write_csv(df: pd.DataFrame, filepath: str):
    csv_str = df.to_csv(index=False)
    async with aiofiles.open(filepath, mode='w') as f:
        await f.write(csv_str)