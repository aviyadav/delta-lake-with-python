import polars as pl
import time


def read_data():
    start_time = time.time()
    df = pl.read_csv("data\\title.basics.tsv.gz", separator='\t', quote_char='', infer_schema_length=1000)
    polars_time = time.time() - start_time
    # print(df.shape)

    print(f"Polars read time: {polars_time:.2f} seconds")
    return df


def write_data_pl(df):
    start_time = time.time()
    # df = df.head(100)
    df.write_delta("tmp/polars-table")
    polars_time = time.time() - start_time
    print(f"Polars write time time: {polars_time:.2f} seconds")


def append_data(df):
    start_time = time.time()
    df.write_delta("tmp/polars-table", mode="append")
    polars_time = time.time() - start_time
    print(f"Polars append time time: {polars_time:.2f} seconds")


def read_data_table():
    start_time = time.time()
    df = pl.read_delta("tmp/polars-table", version=0)
    polars_time = time.time() - start_time
    print(f"Polars read table time: {polars_time:.2f} seconds")
    # print(df.head(5))


if __name__ == '__main__':
    df = read_data()
    write_data_pl(df)
    append_data(df)
    read_data_table()

    """
    Polars read time: 4.65 seconds
    Polars write time time: 9.53 seconds
    Polars append time time: 7.71 seconds
    Polars read table time: 7.25 seconds
    """