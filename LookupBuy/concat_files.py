import pandas as pd
from pathlib import Path

def load_csv2df():
    return pd.concat([ pd.read_csv(filename) for filename in Path().rglob('*.csv')])
