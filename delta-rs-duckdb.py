import duckdb
from deltalake import write_deltalake, DeltaTable
import time


def read_data():
    start_time = time.time()

    csv_file = "data\\title.basics.tsv.gz"
    arrow_table = duckdb.query("""
    select * FROM read_csv('data\\title.basics.tsv.gz', delim='\t', header=True,  AUTO_DETECT=TRUE, quote='', SAMPLE_SIZE=1000000)

    """).to_arrow_table()

    # print(df.head)
    duckdb_time = time.time() - start_time
    print(f"Duckdb read time: {duckdb_time:.2f} seconds")
    return arrow_table


def write_data(arrow_table):
    start_time = time.time()
    write_deltalake(
        data=arrow_table,
        table_or_uri="tmp/duckdb-table",
        mode="overwrite",
    )
    duckdb_time = time.time() - start_time
    print(f"Duckdb write time: {duckdb_time:.2f} seconds")


def append_data(arrow_table):
    start_time = time.time()
    write_deltalake(
        data=arrow_table,
        table_or_uri="tmp/duckdb-table",
        mode="append",
    )

    duckdb_time = time.time() - start_time
    print(f"Duckdb append time: {duckdb_time:.2f} seconds")


def read_duckdb_data():
    start_time = time.time()
    arrow_table = duckdb.query("""
    select * FROM delta_scan('tmp/duckdb-table')
    """).show()
    duckdb_time = time.time() - start_time
    print(f"Duckdb read time: {duckdb_time:.2f} seconds")


if __name__ == '__main__':
    at = read_data()
    write_data(at)
    append_data(at)
    read_duckdb_data()

    """
    Duckdb read time: 7.75 seconds
    Duckdb write time: 6.84 seconds
    Duckdb append time: 6.51 seconds
    Duckdb read time: 1.42 seconds
    """