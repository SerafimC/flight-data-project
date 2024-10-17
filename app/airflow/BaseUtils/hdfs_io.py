from pyspark import SparkConf, SparkContext

class HDFS_IO():

    def __init__(self):
        self.user_path = 'user/jovyan/'
        self.base_url = 'hdfs://namenode:9000/'
        pass

    def write_to_namenode(self, data, target_path:str):
        
        # Initialize SparkConf and SparkContext
        conf = SparkConf().setAppName("WriteToHDFS")
        sc = SparkContext(conf=conf)
        
        # Create an RDD
        rdd = sc.parallelize(data)
        
        # Specify the HDFS path
        hdfs_path = self.base_url + self.user_path + target_path
        
        # Save the RDD to HDFS
        rdd.saveAsTextFile(hdfs_path)
        print('Data stored successfully.')
        
        # Stop SparkContext
        sc.stop()