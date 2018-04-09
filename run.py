from pyspark.sql import SparkSession
from pyspark.sql import Row
import sys
import string
from csv import reader
from functools import reduce
from pyspark.sql import functions as f
from collections import defaultdict
import datetime
from tableCollections import TableCollections

if __name__ == '__main__':
    spark = SparkSession \
                .builder \
                .appName("TableCollections") \
                .config("spark.some.config.option", "some-value") \
                .getOrCreate()
    sc = spark.sparkContext

    parkingTable = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])
    openTable = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[2])

    tc = TableCollections(spark, sc)
    tc.register(openTable, "open")
    tc.register(parkingTable, "parking")
    tc.numColWithinRange(0, 1000000000000).show()
    tc.timeColWithinRange(datetime.datetime(1994,1,1), datetime.datetime(2018,5,1)).show()
