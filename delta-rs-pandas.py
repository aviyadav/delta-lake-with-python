import pandas as pd
from deltalake import write_deltalake, DeltaTable
import time

def pandas_read():
    start_time = time.time()
    df = pd.read_csv("data\\title.basics.tsv.gz", low_memory=False, delimiter="\t")
    # print(df.head)
    pandas_time = time.time() - start_time
    print(f"Pandas read time: {pandas_time:.2f} seconds")
    return df


def pandas_write(df):
    start_time = time.time()
    write_deltalake("tmp/pandas-table", df)
    pandas_time = time.time() - start_time
    print(f"Pandas write time time: {pandas_time:.2f} seconds")


def pandas_append(df):
    start_time = time.time()
    write_deltalake("tmp/pandas-table", df, mode="append")
    pandas_time = time.time() - start_time
    print(f"Pandas append time time: {pandas_time:.2f} seconds")


def pandas_read_table():
    start_time = time.time()
    df = DeltaTable("tmp/pandas-table/").to_pandas()
    pandas_time = time.time() - start_time
    # print(df.head(5))
    print(f"Pandas read table time time: {pandas_time:.2f} seconds")


if __name__ == '__main__':
    df = pandas_read()
    pandas_write(df)
    pandas_append(df)
    pandas_read_table()
    """
    Pandas read time: 32.26 seconds
    Pandas write time time: 15.43 seconds
    Pandas append time time: 15.20 seconds
    Pandas read table time time: 34.77 seconds
    """
