# Instanciate FLightRadarAPI
from FlightRadar24 import FlightRadar24API
fr_api = FlightRadar24API()

zones = fr_api.get_zones()
zones_tuples = [{'name': zone} for zone in zones]

# Spark session
from pyspark.sql import SparkSession
from pyspark.sql import types as T

spark = SparkSession.builder \
    .appName("ZonesExtract") \
    .config("spark.rpc.message.maxSize", "1024") \
    .getOrCreate()

print("Spark version:", spark.version)

zones_schema = T.StructType([
    T.StructField("name", T.StringType(), True)
])
zones_df = spark.createDataFrame(zones_tuples, zones_schema)

destination_path = '/rawobjects/data/zones'
print(destination_path)

zones_df.write.parquet(destination_path, mode='overwrite')
spark.stop()