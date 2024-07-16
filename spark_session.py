from pyspark.sql import SparkSession

def create_spark_session():
    return SparkSession.builder \
        .appName("PGDataReckonerJob") \
        .config("spark.executor.instances", "2") \
        .config("spark.executor.cores", "4") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "1g") \
        .config("spark.dynamicAllocation.enabled", "true") \
        .config("spark.executor.memoryOverhead", "512") \
        .config("spark.driver.memoryOverhead", "512") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()