from pyspark.sql import SparkSession
from pyspark.sql import Row
import sys
import string
from csv import reader
from functools import reduce
from pyspark.sql.functions import *
from collections import defaultdict

class TableCollections:
	def __init__(self):
		self.minVals = dict()
		self.maxVals = dict()

	def register(self, df, name):
		for colName, dtype in df.dtypes:
			if dtype == 'string':
				# figure out distinct values here?
				pass
			elif dtype != 'timestamp':
				self.minVals[name+"."+colName] = df.agg({colName: "min"}).collect()[0][0]
				self.maxVals[name+"."+colName] = df.agg({colName: "max"}).collect()[0][0]

	def returnWithinRange(self, minVal, maxVal):
		# This should be written as a query later, but as a demo purpose, I am writing as a funcction.
		# Should the output be spark DataFrame?
		result = []
		for key, val in self.minVals.items():
			if minVal < val and self.maxVals[key] < maxVal:
				result.append(key)
		return result

spark = SparkSession \
				.builder \
				.appName("TableCollections") \
				.config("spark.some.config.option", "some-value") \
				.getOrCreate()
sc = spark.sparkContext

parkingTable = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[1])
openTable = spark.read.format('csv').options(header='true',inferschema='true').load(sys.argv[2])

tc = TableCollections()
tc.register(openTable, "open")
tc.register(parkingTable, "parking")
#['parking.summons_number', 'open.fine_amount', 'open.summons_number', 'parking.violation_code']
print(tc.returnWithinRange(0, 1000000000000))
