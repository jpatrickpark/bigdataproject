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
        numFileName = name + "_num_metadata.csv"
        timeFileName = name + "_time_metadata.csv"
        stringFileName = name + "_string_metadata.csv"
        string_cols = []
        df.createOrReplaceTempView(name) # can be problematic
        if not self.fs.exists(self.sc._jvm.org.apache.hadoop.fs.Path(numFileName)):
            for colName, dtype in df.dtypes:
                if dtype != 'string' and dtype != 'timestamp':
                    minMax = df.agg(f.min(df[colName]), f.max(df[colName])).collect()[0]
                    metaDf = self.sc.parallelize([
                        (colName,float(minMax[0]),float(minMax[1]))]).toDF(["colName","min","max"])
                    metaDf.write.save(path=numFileName, header="true", format='csv', mode='append', sep = '^')
        else:
            print("num metadata file exists for table {}".format(name))
        if not self.fs.exists(self.sc._jvm.org.apache.hadoop.fs.Path(timeFileName)):
            for colName, dtype in df.dtypes:
                if dtype == 'timestamp':
                    minMax = df.agg(f.min(df[colName]), f.max(df[colName])).collect()[0]
                    metaDf = self.sc.parallelize([
                            (colName,minMax[0].strftime("%Y-%m-%d %H:%M:%S"),minMax[1].strftime("%Y-%m-%d %H:%M:%S"))]).toDF(["colName","min","max"])
                    metaDf.write.save(path=timeFileName, header="true", format='csv', mode='append', sep = '^')
        else:
            print("timestamp metadata file exists for table {}".format(name))
        if not self.fs.exists(self.sc._jvm.org.apache.hadoop.fs.Path(stringFileName)):
            for colName, dtype in df.dtypes:
                if dtype == 'string':
                    string_cols.append(colName)
            self.createStringMetadata(name, string_cols)
        else:
            print("string metadata file exists for table {}".format(name))

    def createStringMetadata(self, df, string_cols):
        name = df + '_string_metadata.csv'
        for col in string_cols:
            query = "SELECT {} as col_value, count(*) as cnt FROM {} GROUP BY {}".format(col, df, col)
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
    
