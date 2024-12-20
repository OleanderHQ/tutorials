from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, FloatType, LongType, StructType, StructField, StringType

sc = SparkContext('local')
spark = SparkSession(sc)

# (1) Creating 'dataforai' iceberg table
schema = StructType([
  StructField("vendor_id", LongType(), True),
  StructField("trip_id", LongType(), True),
  StructField("trip_distance", FloatType(), True),
  StructField("fare_amount", DoubleType(), True),
  StructField("store_and_fwd_flag", StringType(), True)
])

df = spark.createDataFrame([], schema)
df.writeTo("dataforai.sf.waymo").create()

# (2) Write to 'dataforai' iceberg table
schema = spark.table("dataforai.sf.waymo").schema
data = [
    (1, 1000371, 1.8, 15.32, "N"),
    (2, 1000372, 2.5, 22.15, "N"),
    (2, 1000373, 0.9, 9.01, "N"),
    (1, 1000374, 8.4, 42.13, "Y")
  ]
df = spark.createDataFrame(data, schema)
df.writeTo("dataforai.sf.waymo").append()

# (3) Read from 'dataforai' iceberg table
df = spark.table("dataforai.sf.waymo").show()

