# Instanciate FLightRadarAPI
from FlightRadar24 import FlightRadar24API
fr_api = FlightRadar24API()

airlines = fr_api.get_airlines()
airlines_tuples = [{
    'name': obj['Name'],
    'code': obj['Code'],
    'icao': obj['ICAO']
                   } for obj in airlines]

# Spark session
from pyspark.sql import SparkSession
from pyspark.sql import types as T

spark = SparkSession.builder \
    .appName("AirlinesExtract") \
    .config("spark.rpc.message.maxSize", "1024") \
    .getOrCreate()

print("Spark version:", spark.version)

airlines_schema = T.StructType([
    T.StructField("name", T.StringType(), True),
    T.StructField("code", T.StringType(), True),
    T.StructField("icao", T.StringType(), True)
])
airlines_df = spark.createDataFrame(airlines_tuples, airlines_schema)

destination_path = '/rawobjects/data/airlines'
print(destination_path)

airlines_df.write.parquet(destination_path, mode='overwrite')
spark.stop()