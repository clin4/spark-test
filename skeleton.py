import sys
from operator import add
import random

from pyspark.sql import SparkSession


if __name__ == "__main__":
    """
        Usage: Skeleton
    """
    spark = SparkSession\
        .builder\
        .appName("Skeleton")\
        .getOrCreate()

    def f(_):
        return 1 if random.random() > 0.5 else 0

    count = spark.sparkContext.parallelize(range(1, 1000), 5).map(f).reduce(add)
    print("result is roughly %f" % count )

    spark.stop()