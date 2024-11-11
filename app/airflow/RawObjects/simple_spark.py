from pyspark import SparkConf, SparkContext

# Step 1: Create a SparkConf object
conf = SparkConf() \
    .setAppName("PythonVersionTest") \
    .set("spark.rpc.message.maxSize", "1024")

# Step 2: Initialize SparkContext with the SparkConf
sc = SparkContext(conf=conf)

rdd = sc.parallelize([1, 2, 3])
rdd.map(lambda x: x * 2).collect()

print('completed')
