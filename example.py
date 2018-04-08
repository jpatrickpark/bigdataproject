from pyspark.sql import SparkSession
from pyspark.sql import Row
import sys
import string
from csv import reader
from functools import reduce
from pyspark.sql import functions as f
from collections import defaultdict
import datetime

class TableCollections:
    def __init__(self,spark):
        self.spark = spark
        self.minNums = dict()
        self.maxNums = dict()
        self.minTimes = dict()
        self.maxTimes = dict()

    def register(self, df, name):
        for colName, dtype in df.dtypes:
            if dtype == 'string':
                # figure out distinct values here?
                pass
            elif dtype == 'timestamp':
                minMax = df.agg(f.min(df[colName]), f.max(df[colName])).collect()[0]
                self.minTimes[name+"."+colName] = minMax[0]
                self.maxTimes[name+"."+colName] = minMax[1]
            else:
                minMax = df.agg(f.min(df[colName]), f.max(df[colName])).collect()[0]
                self.minNums[name+"."+colName] = minMax[0]
                self.maxNums[name+"."+colName] = minMax[1]

    def timeColWithinRange(self, minTime, maxTime):
        result = []
        if type(minTime) != datetime.datetime or type(maxTime) != datetime.datetime:
            raise TypeError("minNum, maxNum must be timestamp")
        # Figure out a way to return the result as DataFrame.
        # I can save a csv for each dataframe. 
        # columns will be (colName, min, max)
        for key, val in self.minTimes.items():
            if type(val) == datetime.datetime:
                if minTime < val and self.maxTimes[key] < maxTime:
                    result.append(key)
        # create spark dataframe here
        return result

    def numColWithinRange(self, minNum, maxNum):
        # int, bigint, float, long
        if type(minNum) == datetime.datetime or \
            type(minNum) == str or \
            type(maxNum) == datetime.datetime or \
            type(maxNum) == str:
            raise TypeError("minNum, maxNum must be number")
        result = []
        for key, val in self.minNums.items():
            if minNum < val and self.maxNums[key] < maxNum:
                result.append(key)
        # create spark dataframe here
        return result


spark = SparkSession \
                .builder \
                .appName("TableCollections") \
                .config("spark.some.config.option", "some-value") \
                .getOrCreate()
sc = spark.sparkContext

parkingTable = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])
openTable = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[2])

tc = TableCollections(spark)
tc.register(openTable, "open")
tc.register(parkingTable, "parking")
#['parking.summons_number', 'open.fine_amount', 'open.summons_number', 'parking.violation_code']
print(tc.numColWithinRange(0, 1000000000000))
#['parking.issue_date']
print(tc.timeColWithinRange(datetime.datetime(1994,1,1), datetime.datetime(2018,5,1)))
