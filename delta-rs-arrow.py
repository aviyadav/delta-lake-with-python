import pyarrow.csv as pv
import pyarrow.parquet as pq
from pyarrow import csv
from deltalake.writer import write_deltalake
from deltalake import DeltaTable, write_deltalake
import time

def read_data():
    start_time = time.time()
    csv_file_path = "data\\title.basics.tsv.gz"

    parse_options = csv.ParseOptions(delimiter="\t", quote_char=False, )

    table = pv.read_csv(csv_file_path, parse_options=parse_options)

    exec_time = time.time() - start_time
    print(f"Arrow read time: {exec_time:.2f} seconds")
    return table


def write_data(table):
    start_time = time.time()
    write_deltalake("tmp/pyarrow_table", table)
    exec_time = time.time() - start_time
    print(f"Arrow write time: {exec_time:.2f} seconds")


def append_data(table):
    start_time = time.time()
    write_deltalake("tmp/pyarrow_table", table, mode="append")
    exec_time = time.time() - start_time
    print(f"Arrow append time: {exec_time:.2f} seconds")


def delta_to_pandas_read():
    start_time = time.time()
    dt = DeltaTable("tmp/pyarrow_table")
    df = dt.to_pandas()
    # print(df.head(5))
    exec_time = time.time() - start_time
    print(f"Delta to pandas read time: {exec_time:.2f} seconds")


def delta_to_arrow_read():
    start_time = time.time()
    dt = DeltaTable("tmp/pyarrow_table")
    df = dt.to_pyarrow_table()
    # print(df)
    exec_time = time.time() - start_time
    print(f"Delta to pyarrow read time: {exec_time:.2f} seconds")


if __name__ == '__main__':
    pyarrow_table = read_data()
    write_data(pyarrow_table)
    append_data(pyarrow_table)
    delta_to_pandas_read()
    delta_to_arrow_read()

    """
    Arrow read time: 3.61 seconds
    Arrow write time: 7.94 seconds
    Arrow append time: 6.70 seconds
    Delta to pandas read time: 36.49 seconds
    Delta to pyarrow read time: 3.17 seconds
    """