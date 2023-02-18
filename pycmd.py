import pyspark
from pyspark.sql import SparkSession
import pyarrow as pa
import polars as pl
import io

from typing import Iterator

def polars_transform(df: pl.DataFrame) -> pl.DataFrame:
    return df.select([
        pl.col('letter') + pl.lit('!'),
    ])

def arrow_transform(iter: Iterator[pa.RecordBatch]) -> Iterator[pa.RecordBatch]:
    # Transform a single RecordBatch so data fit into memory
    # Increase spark.sql.execution.arrow.maxRecordsPerBatch if batches are too small
    for batch in iter:
        polars_df = pl.from_arrow(pa.Table.from_batches([batch]))
        #polars_df = polars_df.with_column(pl.col("letter").cast(pl.Utf8))

        #f = io.BytesIO()
        #polars_df.write_ipc(f)
        #f.seek(0)
        #print("yoooooooooooooooooooooooooooooooo")
        #print(pl.read_ipc(f).dtypes)
        polars_df_2 = polars_transform(polars_df)
        arr = polars_df_2.to_arrow()
        for b in arr.to_batches():
            bb = pa.RecordBatch.from_arrays([b.column(0).cast(pa.utf8())], names=['letter'])
            #b.set_column(0, b.column(0).cast(pa.utf8()))
            yield bb #.cast(pa.utf8())

spark = SparkSession.builder \
    .appName('demo_pca') \
    .getOrCreate()

letters = [{'letter': 'a'}, {'letter': 'b'}, {'letter': 'c'}]
df = spark.createDataFrame(letters)
df = df.mapInArrow(arrow_transform, schema='letter string')
df.createOrReplaceTempView('letters')

print("showing dataframe")
df.show()