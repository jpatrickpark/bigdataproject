from pyspark.sql import SparkSession
from pyspark.sql import Row
import sys
import string
from csv import reader
from functools import reduce
from pyspark.sql import functions as f
from collections import defaultdict
import datetime
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, TimestampType, StringType

class TableCollections:
    def __init__(self,spark,sc):
        self.spark = spark
        self.sc = sc
        self.tableNames = []
        self.fs = self.sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())

    def add_registered_table_name(self, name):
        numFileName = name + "_num_metadata.csv"
        timeFileName = name + "_time_metadata.csv"
        stringFileName = name + "_string_metadata.csv"
        if  self.fs.exists(self.sc._jvm.org.apache.hadoop.fs.Path(numFileName)) or \
            self.fs.exists(self.sc._jvm.org.apache.hadoop.fs.Path(timeFileName)) or \
            self.fs.exists(self.sc._jvm.org.apache.hadoop.fs.Path(stringFileName)):
            self.tableNames.append(name)
            return True
        return False

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
        num_cols, time_cols, string_cols, bool_cols = [], [], [], []
        df.createOrReplaceTempView(name) # can be problematic

        # put column names into appropriate bin
        for colName, dtype in df.dtypes:
            if dtype == 'timestamp':
                time_cols.append(colName)
            elif dtype == 'string':
                string_cols.append(colName)
            elif dtype == 'boolean':
                bool_cols.append(colName)
            else:
                num_cols.append(colName)

        # For each datatype of columns, process metadata
        if not self.fs.exists(self.sc._jvm.org.apache.hadoop.fs.Path(numFileName)):
            self.createNumMetadata(df, num_cols, numFileName)
            self.createBoolMetadata(df, bool_cols, numFileName)
        else:
            print("num metadata file exists for table {}".format(name))
        if not self.fs.exists(self.sc._jvm.org.apache.hadoop.fs.Path(timeFileName)):
            self.createTimeMetadata(df, time_cols, timeFileName)
        else:
            print("timestamp metadata file exists for table {}".format(name))
        if not self.fs.exists(self.sc._jvm.org.apache.hadoop.fs.Path(stringFileName)):
            #pass
            self.createStringMetadata(name, string_cols)
        else:
            print("string metadata file exists for table {}".format(name))

    def createBoolMetadata(self, df, bool_cols, boolFilename):
        for colName in bool_cols:
            minMax = df.agg(f.min(df[colName]), f.max(df[colName])).collect()[0]
            metaDf = self.sc.parallelize([
                    (colName,float(minMax[0]),float(minMax[1]))]).toDF(["colName","min","max"])
            metaDf.write.save(path=boolFilename, header="false", format='csv', mode='append', sep = '^')

    def createTimeMetadata(self, df, time_cols, timeFileName):
        for colName in time_cols:
            minMax = df.agg(f.min(df[colName]), f.max(df[colName])).collect()[0]
            metaDf = self.sc.parallelize([
                    (colName,minMax[0].strftime("%Y-%m-%d %H:%M:%S"),minMax[1].strftime("%Y-%m-%d %H:%M:%S"))]).toDF(["colName","min","max"])
            metaDf.write.save(path=timeFileName, header="false", format='csv', mode='append', sep = '^')

    def createNumMetadata(self, df, num_cols, numFileName):
        describeTable = df[num_cols].describe().collect()

        for colName in num_cols:
            metaDf = self.sc.parallelize([
                     (colName,float(describeTable[3][colName]),float(describeTable[4][colName]))]).toDF(["colName","min","max"])
            metaDf.write.save(path=numFileName, header="false", format='csv', mode='append', sep = '^')

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

        schema = StructType([
            StructField("colName", StringType(), True),
            StructField("min", TimestampType(), True),
            StructField("max", TimestampType(), True)])

        for each in self.tableNames:
            filename = each + '_time_metadata.csv'
            if self.fs.exists(self.sc._jvm.org.apache.hadoop.fs.Path(filename)):
                currentTable = self.spark.read.csv(filename,header=False,schema=schema, sep='^')
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

        schema = StructType([
            StructField("colName", StringType(), True),
            StructField("min", DoubleType(), True),
            StructField("max", DoubleType(), True)])

        for each in self.tableNames:
            filename = each + '_num_metadata.csv'
            if self.fs.exists(self.sc._jvm.org.apache.hadoop.fs.Path(filename)):
                currentTable = self.spark.read.csv(filename,header=False,schema=schema, sep='^')
                if not resultCreated:
                    resultDf = currentTable.where(currentTable.min>minNum).where(currentTable.max<maxNum).select(currentTable.colName).withColumn("tableName", f.lit(each))
                    resultCreated = True
                else:
                    resultDf = resultDf.union(currentTable.where(currentTable.min>minNum).where(currentTable.max<maxNum).select(currentTable.colName).withColumn("tableName",f.lit(each)))
        return resultDf

    def getNumRange(self,colList):
        resultCreated = False
        # colList element format: tableName^colName
        schema = StructType([
            StructField("colName", StringType(), True),
            StructField("min", DoubleType(), True),
            StructField("max", DoubleType(), True)])
        for each in colList:
            tableName, colName = each.split('^',1)
            filename = tableName + '_num_metadata.csv'
            if self.fs.exists(self.sc._jvm.org.apache.hadoop.fs.Path(filename)):
                currentTable = self.spark.read.csv(filename,header=False,schema=schema, sep='^')
                if not resultCreated:
                    newDf = currentTable.where(currentTable.colName==colName).withColumn("tableName", f.lit(tableName))
                    resultCreated = True
                else:
                    newDf = newDf.union(currentTable.where(currentTable.colName==colName).withColumn("tableName", f.lit(tableName)))
        resultDf = newDf.select(["tableName","colName","min","max"])
        return resultDf

    def getTimeRange(self, colList):
        resultCreated = False
        # colList element format: tableName^colName
        schema = StructType([
            StructField("colName", StringType(), True),
            StructField("min", TimestampType(), True),
            StructField("max", TimestampType(), True)])
        for each in colList:
            tableName, colName = each.split('^',1)
            filename = tableName + '_time_metadata.csv'
            if self.fs.exists(self.sc._jvm.org.apache.hadoop.fs.Path(filename)):
                #print(tableName)
                currentTable = self.spark.read.csv(filename,header=False,schema=schema, sep='^')
                if not resultCreated:
                    newDf = currentTable.where(currentTable.colName==colName).withColumn("tableName", f.lit(tableName))
                    resultCreated = True
                else:
                    newDf = newDf.union(currentTable.where(currentTable.colName==colName).withColumn("tableName", f.lit(tableName)))
        resultDf = newDf.select(["tableName","colName","min","max"])
        return resultDf

    def getSimilarNumCols(self, tableColName, threshold=0):
        resultCreated = False
        schema = StructType([
            StructField("colName", StringType(), True),
            StructField("min", DoubleType(), True),
            StructField("max", DoubleType(), True)])

        tableName, colName = tableColName.split('^',1) # check for possible error? Maybe after merging functions
        filename = tableName + '_num_metadata.csv'
        if not self.fs.exists(self.sc._jvm.org.apache.hadoop.fs.Path(filename)):
            raise LookupError("Given column does not exist!")
        currentTable = self.spark.read.csv(filename,header=False,schema=schema, sep='^')
        minMax = currentTable.where(currentTable.colName == colName).collect()[0]
        currentMin = minMax["min"]
        currentMax = minMax["max"]
        resultDf = None
        for each in self.tableNames:
            filename = each + '_num_metadata.csv'
            if self.fs.exists(self.sc._jvm.org.apache.hadoop.fs.Path(filename)):
                currentTable = self.spark.read.csv(filename,header=False,schema=schema, sep='^')
                rows = currentTable.rdd.collect()
                for row in rows:
                    intersection = min(row["max"], currentMax) - max(row["min"], currentMin)
                    if intersection <= 0:
                        continue
                    union = max(row["max"], currentMax) - min(row["min"], currentMin)
                    iou = intersection / union
                    if iou <= threshold:
                        continue
                    if not resultCreated:
                        resultDf = currentTable.where(currentTable.colName == row["colName"]).select(currentTable.colName).withColumn("tableName", f.lit(each)).withColumn("iou", f.lit(iou))
                        resultCreated = True
                    else:
                        resultDf = resultDf.union(currentTable.where(currentTable.colName == row["colName"]).select(currentTable.colName).withColumn("tableName", f.lit(each)).withColumn("iou", f.lit(iou)))
        if resultDf == None:
            resultSchema = StructType([
                StructField("tableName", StringType(), True),
                StructField("colName", StringType(), True),
                StructField("iou", DoubleType(), True)])
            return self.spark.createDataFrame(self.sc.emptyRDD(), resultSchema)
        return resultDf.sort(f.desc("iou"))

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

    def returnOutliers(self, colList, percentage):
        result = []
        for each in colList:
            tableName, colName = each.split('^', 1)
            filename = tableName + '_string_metadata.csv'
            if self.fs.exists(self.sc._jvm.org.apache.hadoop.fs.Path(filename)):
                currentTable = self.spark.read.format('csv').options(header='true',inferschema='true', sep = '^').load(filename)
                newDf = currentTable.where(currentTable.col_name==colName)
                numRows = newDf.count()
                numOutliers = round(numRows * percentage)
                newDf = newDf.sort(f.asc("cnt")).limit(numOutliers)
                result.append(newDf)
        for df in result:
            df.show()

    # A function that takes two lists of column values list A and list B as
    # input and returns a list of table_name, column_name of columns where all
    # the elements in A are present but any of the elements in B are not present.
    def colsWithAndWithout(self, colList, withList, withoutList):
        result = []
        for each in colList:
            tableName, colName = each.split('^', 1)
            filename = tableName + '_string_metadata.csv'
            if self.fs.exists(self.sc._jvm.org.apache.hadoop.fs.Path(filename)):
                currentTable = self.spark.read.format('csv').options(header='true',inferschema='true', sep = '^').load(filename)
                newDf = currentTable.where(currentTable.col_name==colName)

                # check if any excluding words are in the column
                if len(newDf.filter(f.col('col_value').isin(withoutList)).head(1)) == 0:
                    # check if all including words are in the column
                    withCnt = 0
                    for word in withList:
                        if newDf.filter(newDf.col_value == word).count() > 0:
                            withCnt += 1
                    if withCnt == len(withList):
                        result.append(each)
        if len(result) == 0:
            print("There are no columns that satisfies the condition")
        else:
            print("tablename^columname that satisfies the condition are: ")
            print(*result, sep = ", ")

    def getCardinality(self, colList):
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
        resultDf = newDf.groupBy(newDf.col_name).count()
        return resultDf

    def getColsOfDatatype(self, df, coltype):
        
        # Clean up column names so that we can prevent future errors
        for colName, dtype in df.dtypes:
            if '.' in colName or '`' in colName or colName.strip() != colName:
                df = df.withColumnRenamed(colName, colName.strip().replace(".", "").replace("`", ""))

        time_cols, string_cols, bool_cols, num_cols = []

        # put column names into appropriate bin
        for colName, dtype in df.dtypes:
            if dtype == 'timestamp':
                time_cols.append(colName)
            elif dtype == 'string':
                string_cols.append(colName)
            elif dtype == 'boolean':
                bool_cols.append(colName)
            else:
                num_cols.append(colName)

        if(coltype != 'all'):
            if(coltype == 'string' or coltype == 'String'):
                if(string_cols):
                    print("Columns of data type String are: ",string_cols)
            elif(coltype == 'int' or coltype == 'Integer'):
                if(num_cols):
                    print("Columns of data type Integer are: ",string_cols)
            elif(coltype == 'boolean' or coltype == 'Boolean'):
                if(bool_cols):
                    print("Columns of data type Boolean are: ",bool_cols)
            elif(coltype == 'time' or coltype == 'Time'):
                if(time_cols):
                    print("Columns of data type Time are: ",time_cols)
        else:
            if(string_cols != []):
                print("Columns of data type String are: ",string_cols)
            else:
                print("There are no string columns")

            if(num_cols != []):
                print("Columns of data type Integer are: ",string_cols)
            else:
                print("There are no Integer columns")

            if(bool_cols != []):
                    print("Columns of data type Boolean are: ",bool_cols)
            else:
                print("There are no Boolean columns")
            
            if(time_cols != []):
                print("Columns of data type Time are: ",time_cols)
            else:
                print("There are no Time columns")

    def colsOfcategory(self, df, category = None, df2=None):
        # Clean up column names so that we can prevent future errors
        for colName, dtype in df.dtypes:
            if '.' in colName or '`' in colName or colName.strip() != colName:
                df = df.withColumnRenamed(colName, colName.strip().replace(".", "", "_").replace("`", ""))
        if(df2 == None):

            result_df = pd.DataFrame(columns= ['Column_1','Path Similarity(Between 0 to 1)'])
            category_sys = wn.synsets(category)
            if(category_sys != []):
                cnt = 0
                # put column names into appropriate bin
                for colName, dtype in df.dtypes:
                    colName_ = colName.split("_")
                    for i in range(len(colName_)):
                        score = []
                        colName_sys = wn.synsets(colName_[i])
                        if(colName_sys != []):
                            score.append(colName_sys[0].path_similarity(category_sys[0]))
                    if(score != []):
                        score = max(score)
                    else:
                        score = "Simlarity cannot be calculated"
                    result_df.loc[cnt] = [colName, score]
                    cnt += 1
            else:
                print("Similarity cannot be calculated")
            result_df = result_df[result_df['Path Similarity(Between 0 to 1)'] > 0.5]
            return result_df
        else:
            for colName, dtype in df2.dtypes:
                if '.' in colName or '`' in colName or colName.strip() != colName:
                    df2 = df2.withColumnRenamed(colName, colName.strip().replace(".", "", "_").replace("`", ""))
            result_df = pd.DataFrame(columns= ['Column_1', 'Column_2','Path Similarity(Between 0 to 1)'])
            cnt = 0
            # put column names into appropriate bin
            for colName1, dtype in df.dtypes:
                colName_1 = colName1.split("_")
                for colName2, dtype2 in df2.dtypes:
                    colName_2 = colName2.split("_")
                    score = []
                    #print(colName_1, colName_2, score)
                    for i in range(len(colName_1)):
                        colName_sys_1 = wn.synsets(colName_1[i])
                        for j in range(len(colName_2)):
                            colName_sys_2 = wn.synsets(colName_2[j])
                            if(colName_sys_1 != [] and colName_sys_2 != []):
                                score.append(colName_sys_1[0].path_similarity(colName_sys_2[0]))
                    if(score != []):
                        score = max(score)
                    else:
                        score = "Simlarity cannot be calculated"
                    result_df.loc[cnt] = [colName1, colName2, score]
                    cnt += 1
            result_df = result_df[result_df['Path Similarity(Between 0 to 1)'] > 0.5]
            return result_df