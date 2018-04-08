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
    def __init__(self,spark,sc):
        self.spark = spark
        self.sc = sc
        self.tableNames = []
        self.fs = self.sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())

    def register(self, df, name):
        self.tableNames.append(name)
        numFileName = name+"_num_metadata.csv"
        timeFileName = name+"_time_metadata.csv"
        if not self.fs.exists(self.sc._jvm.org.apache.hadoop.fs.Path(numFileName)):
            for colName, dtype in df.dtypes:
                if dtype != 'string' and dtype != 'timestamp':
                    minMax = df.agg(f.min(df[colName]), f.max(df[colName])).collect()[0]
                    #self.minNums[name+"."+colName] = minMax[0]
                    #self.maxNums[name+"."+colName] = minMax[1]
                    metaDf = self.sc.parallelize([
                        (colName,float(minMax[0]),float(minMax[1]))]).toDF(["colName","min","max"])
                    metaDf.write.save(path=numFileName, format='csv', mode='append', sep=',')
        else:
            print("file exists")
        if not self.fs.exists(self.sc._jvm.org.apache.hadoop.fs.Path(timeFileName)):
            for colName, dtype in df.dtypes:
                if dtype == 'timestamp':
                    minMax = df.agg(f.min(df[colName]), f.max(df[colName])).collect()[0]
                    #self.minTimes[name+"."+colName] = minMax[0]
                    #self.maxTimes[name+"."+colName] = minMax[1]
                    metaDf = self.sc.parallelize([
                            (colName,minMax[0].strftime("%Y-%m-%d %H:%M:%S"),minMax[1].strftime("%Y-%m-%d %H:%M:%S"))]).toDF(["colName","min","max"])
                    metaDf.write.save(path=timeFileName, format='csv', mode='append', sep=',')

    def timeColWithinRange(self, minTime, maxTime):
        resultCreated = False
        if type(minTime) != datetime.datetime or type(maxTime) != datetime.datetime:
            raise TypeError("minNum, maxNum must be timestamp")
        
        for each in self.tableNames:
            filename = each + '_time_metadata.csv'
            if self.fs.exists(self.sc._jvm.org.apache.hadoop.fs.Path(filename)):
                currentTable = spark.read.format('csv').options(header='false',inferschema='true').load(filename)
                oldColumns = currentTable.schema.names
                newColumns = ["colName","min","max"]
                currentTable = reduce(lambda currentTable, idx: currentTable.withColumnRenamed(oldColumns[idx], newColumns[idx]), range(len(oldColumns)), currentTable)
                if not resultCreated:
                    resultDf = currentTable.where(currentTable.min>minTime).where(currentTable.max<maxTime).select(currentTable.colName).withColumn("tableName", f.lit(each))
                    resultCreated = True
                else:
                    resultDf = resultDf.union(currentTable.where(currentTable.min>minTime).where(currentTable.max<maxTime).select(currentTable.colName).withColumn("tableName", f.lit(each)))

        return resultDf

    def numColWithinRange(self, minNum, maxNum):
        # int, bigint, float, long
        resultCreated = False
        if type(minNum) == datetime.datetime or \
            type(minNum) == str or \
            type(maxNum) == datetime.datetime or \
            type(maxNum) == str:
            raise TypeError("minNum, maxNum must be number")
        for each in self.tableNames:
            filename = each + '_num_metadata.csv'
            if self.fs.exists(self.sc._jvm.org.apache.hadoop.fs.Path(filename)):
                currentTable = spark.read.format('csv').options(header='false',inferschema='true').load(filename)
                oldColumns = currentTable.schema.names
                newColumns = ["colName","min","max"]
                currentTable = reduce(lambda currentTable, idx: currentTable.withColumnRenamed(oldColumns[idx], newColumns[idx]), range(len(oldColumns)), currentTable)
                if not resultCreated:
                    resultDf = currentTable.where(currentTable.min>minNum).where(currentTable.max<maxNum).select(currentTable.colName).withColumn("tableName", f.lit(each))
                    resultCreated = True
                else:
                    resultDf = resultDf.union(currentTable.where(currentTable.min>minNum).where(currentTable.max<maxNum).select(currentTable.colName).withColumn("tableName",f.lit(each)))
        return resultDf


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
