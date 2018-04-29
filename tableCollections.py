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
        # Clean up column names so that we can prevent future errors
        for colName, dtype in df.dtypes:
            if '.' in colName or '`' in colName or colName.strip() != colName:
                df = df.withColumnRenamed(colName, colName.strip().replace(".", "").replace("`", ""))

        # track down which tables have been registered to the class
        self.tableNames.append(name)
        numFileName = name + "_num_metadata.csv"
        timeFileName = name + "_time_metadata.csv"
        stringFileName = name + "_string_metadata.csv"
        num_cols, time_cols, string_cols = [], [], []
        df.createOrReplaceTempView(name) # can be problematic

        # put column names into appropriate bin
        for colName, dtype in df.dtypes:
            if dtype == 'timestamp':
                time_cols.append(colName)
            elif dtype == 'string':
                string_cols.append(colName)
            else:
                num_cols.append(colName)

        # For each datatype of columns, process metadata
        if not self.fs.exists(self.sc._jvm.org.apache.hadoop.fs.Path(numFileName)):
            self.createNumMetadata(df, num_cols, numFileName)
        else:
            print("num metadata file exists for table {}".format(name))
        if not self.fs.exists(self.sc._jvm.org.apache.hadoop.fs.Path(timeFileName)):
            self.createTimeMetadata(df, time_cols, timeFileName)
        else:
            print("timestamp metadata file exists for table {}".format(name))
        if not self.fs.exists(self.sc._jvm.org.apache.hadoop.fs.Path(stringFileName)):
            self.createStringMetadata(name, string_cols)
        else:
            print("string metadata file exists for table {}".format(name))

    def createTimeMetadata(self, df, time_cols, timeFileName):
        for colName in time_cols:
            minMax = df.agg(f.min(df[colName]), f.max(df[colName])).collect()[0]
            metaDf = self.sc.parallelize([
                    (colName,minMax[0].strftime("%Y-%m-%d %H:%M:%S"),minMax[1].strftime("%Y-%m-%d %H:%M:%S"))]).toDF(["colName","min","max"])
            metaDf.write.save(path=timeFileName, header="true", format='csv', mode='append', sep = '^')

    def createNumMetadata(self, df, num_cols, numFileName):
        for colName in num_cols:
            minMax = df.agg(f.min(df[colName]), f.max(df[colName])).collect()[0]
            metaDf = self.sc.parallelize([
                     (colName,float(minMax[0]),float(minMax[1]))]).toDF(["colName","min","max"])
            metaDf.write.save(path=numFileName, header="true", format='csv', mode='append', sep = '^')

    def createStringMetadata(self, df, string_cols):
        name = df + '_string_metadata.csv'
        for col in string_cols:
            query = "SELECT `{}` as col_value, count(*) as cnt FROM {} GROUP BY `{}`".format(col, df, col)
            x = self.spark.sql(query)
            x = x.withColumn("col_name", f.lit(col))
            x.coalesce(1).write.save(path = name, header= "true", mode = "append", format = "com.databricks.spark.csv", sep = '^')

    def timeColWithinRange(self, minTime, maxTime):
        resultCreated = False
        if type(minTime) != datetime.datetime or type(maxTime) != datetime.datetime:
            raise TypeError("minNum, maxNum must be timestamp")

        for each in self.tableNames:
            filename = each + '_time_metadata.csv'
            if self.fs.exists(self.sc._jvm.org.apache.hadoop.fs.Path(filename)):
                currentTable = self.spark.read.format('csv').options(header='true',inferschema='true', sep = '^').load(filename)
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
                currentTable = self.spark.read.format('csv').options(header='true',inferschema='true', sep = '^').load(filename)
                if not resultCreated:
                    resultDf = currentTable.where(currentTable.min>minNum).where(currentTable.max<maxNum).select(currentTable.colName).withColumn("tableName", f.lit(each))
                    resultCreated = True
                else:
                    resultDf = resultDf.union(currentTable.where(currentTable.min>minNum).where(currentTable.max<maxNum).select(currentTable.colName).withColumn("tableName",f.lit(each)))
        return resultDf

    def returnIntersecWithinCols(self,colList):
        resultCreated = False
        # colList element format: tableName^colName
        for each in colList:
            tableName, colName = each.split('^',1);
            filename = tableName + '_string_metadata.csv'
            if self.fs.exists(self.sc._jvm.org.apache.hadoop.fs.Path(filename)):
                currentTable = self.spark.read.format('csv').options(header='true',inferschema='true', sep = '^').load(filename)
                if not resultCreated:
                    newDf = currentTable.where(currentTable.col_name==colName)
                    resultCreated = True
                else:
                    newDf = newDf.union(currentTable.where(currentTable.col_name==colName))
        resultDf = newDf.groupBy(newDf.col_value).count()
        resultDf = resultDf.filter(resultDf["count"] == len(colList))
        return resultDf

    # A function that takes a list of columns and an integer N as input and
    # returns a list of N rows of column value and frequency for the top N
    # frequent column values in each column.
    def frequentVals(self, colList, topN):
        resultCreated = False
        # colList element format: tableName^colName
        for each in colList:
            tableName, colName = each.split('^', 1)
            filename = tableName + '_string_metadata.csv'
            if self.fs.exists(self.sc._jvm.org.apache.hadoop.fs.Path(filename)):
                currentTable = self.spark.read.format('csv').options(header='true',inferschema='true', sep = '^').load(filename)
                if not resultCreated:
                    newDf = currentTable.where(currentTable.col_name==colName)
                    resultCreated = True
                else:
                    newDf = newDf.union(currentTable.where(currentTable.col_name==colName))
        resultDf = newDf.sort(f.desc("cnt")).limit(topN)
        return resultDf
