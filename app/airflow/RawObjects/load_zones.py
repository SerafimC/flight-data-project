from pyspark.sql import functions as F
import sys, os
path = os.path.dirname(os.getcwd()) + '/BaseUtils/'
sys.path.append(os.path.abspath(path))

from hdfs_io import *
hdfs_obj = HDFS_IO()

print('hdfs_obj type: ' + str(type(hdfs_obj)))

# Instanciate FLightRadarAPI
from FlightRadar24 import FlightRadar24API
fr_api = FlightRadar24API()

zones = fr_api.get_zones()
zones_tuples = [{'name': zone} for zone in zones]

# Spark session
from pyspark.sql import SparkSession
from pyspark.sql import types as T

spark = SparkSession.builder \
    .appName("LocalSpark") \
    .master("spark://spark-master:7077") \
    .config("spark.rpc.message.maxSize", "1024") \
    .getOrCreate()

print("Spark version:", spark.version)

zones_schema = T.StructType([
    T.StructField("name", T.StringType(), True)
])
zones_df = spark.createDataFrame(zones_tuples, zones_schema)

destination_path = hdfs_obj.base_url + hdfs_obj.user_path + 'FlightRadarApi/zones'
print(destination_path)

zones_df.write.parquet(destination_path, mode='overwrite')
spark.stop()