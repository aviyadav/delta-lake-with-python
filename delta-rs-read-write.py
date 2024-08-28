import pandas as pd
from deltalake import write_deltalake, DeltaTable
import time
import pyarrow

def create_data():
    df = pd.DataFrame(
        {
            "product_id": ["1", "2", "3", "4"],
            "product": ["cucumber", "water melon", "broculi", "tomatoes"],
            "sales_price": [1.25, 2, 1, 2],
            "qt": [1, 1, 1, 1],
            "available": [True, True, True, True],
        }
    )

    df.columns = df.columns.str.lower()
    print(df.head(100))
    write_deltalake("tmp/vegetables", df)


def read_data():
    dt = DeltaTable("tmp/vegetables")
    print(dt.to_pandas().head(100))


def update_data():
    dt = DeltaTable("tmp/vegetables")

    dt.update(updates={"qt": "10"}, predicate="sales_price > 0.0")
    print(dt.to_pandas().head(100))

    dt = DeltaTable("tmp/vegetables")
    dt.update(updates={"available": "False"}, predicate="product_id = '1'")


def add_new_data():
    # write_deltalake("tmp/vegetables", df, mode="append")
    pass


def merge_data():
    dt = DeltaTable("tmp/vegetables")
    df = pd.DataFrame(
        {
            "product_id": ["1", "2", "3", "4", "5"],
            "product": ["cucumber", "water melon", "broculi", "tomatoes", "cabbage"],
            "sales_price": [3.1, 3.2, 3.3, 3.4, 1.75],
            "qt": [10, 9, 8, 7, 20],
            "available": [True, True, True, True, True],
        }
    )
    table = pyarrow.Table.from_pandas(df, preserve_index=False)

    (
        dt.merge(
            source=table,
            predicate="s.product_id = t.product_id",
            source_alias="s",
            target_alias="t",
        )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )


if __name__ == '__main__':
    # create_data()
    read_data()
    # update_data()
    # add_new_data()
    # merge_data()
    
    
    
    
    