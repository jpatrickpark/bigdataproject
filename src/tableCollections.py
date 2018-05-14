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
from nltk.corpus import wordnet as wn
import pandas as pd

class TableCollections:

    def __init__(self,spark,sc):
        # initialize spark and sql context
        self.spark = spark
        self.sc = sc
        # a list to store all the table names that have been registered and have metadata created
        self.tableNames = []
        self.fs = self.sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())

    # add newly registered table to tableNames list
    def add_registered_table_name(self, name):

        """
            :param name: tableName given by the user
            :return: True if metadata files are registered, else False
        """

        numFileName = name + "_num_metadata.csv"
        timeFileName = name + "_time_metadata.csv"
        stringFileName = name + "_string_metadata.csv"
        if  self.fs.exists(self.sc._jvm.org.apache.hadoop.fs.Path(numFileName)) or \
            self.fs.exists(self.sc._jvm.org.apache.hadoop.fs.Path(timeFileName)) or \
            self.fs.exists(self.sc._jvm.org.apache.hadoop.fs.Path(stringFileName)):
            self.tableNames.append(name)
            return True
        return False

    # create metadata depending on the datatype -- timestamp, string, boolean
    def register(self, df, name):
        """
            :param df: spark dataframe
            :param name: name of table given by the user

        """
        # clean up column names to prevent future errors
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

        # for each datatype of columns, process metadata
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
            self.createStringMetadata(name, string_cols)
        else:
            print("string metadata file exists for table {}".format(name))

    def createBoolMetadata(self, df, bool_cols, bool_filename):
        """
            :param df: spark dataframe
            :param bool_cols: list of boolean columns in dataframe
            :param name: name of metadata file to be created

        """
        for colName in bool_cols:
            query = "SELECT `{}` as col_value, count(*) as cnt FROM {} GROUP BY `{}`".format(colName, df, colName)
            x = self.spark.sql(query)
            x = x.withColumn("col_name", f.lit(colName))
            x.coalesce(1).write.save(path = bool_filename, header= "true", mode = "append", format = "csv", sep = '^')

    def createTimeMetadata(self, df, time_cols, time_filename):
        """
            :param df: spark dataframe
            :param time_filename: list of boolean columns in dataframe
            :param name: name of metadata file to be created
            Extracts the min and maximum date from time using describe function
        """
        for colName in time_cols:
            minMax = df.agg(f.min(df[colName]), f.max(df[colName])).collect()[0]
            metaDf = self.sc.parallelize([
                    (colName,minMax[0].strftime("%Y-%m-%d %H:%M:%S"),minMax[1].strftime("%Y-%m-%d %H:%M:%S"))]).toDF(["colName","min","max"])
            metaDf.write.save(path=time_filename, header="false", format='csv', mode='append', sep = '^')

    def createNumMetadata(self, df, num_cols, num_filename):
        """
            :param df: spark dataframe
            :param num_cols: list of numerical columns in dataframe
            :param name: name of metadata file to be created
            Extracts the min and maximum value from numerical columns using describe function

        """
        describeTable = df[num_cols].describe().collect()
        for colName in num_cols:
            metaDf = self.sc.parallelize([
                     (colName,float(describeTable[3][colName]),float(describeTable[4][colName]))]).toDF(["colName","min","max"])
            metaDf.write.save(path=num_filename, header="false", format='csv', mode='append', sep = '^')

    def createStringMetadata(self, df, string_cols):
        """
            :param df: spark dataframe
            :param string_cols: list of boolean columns in dataframe
            Extracts all the distinct value and count using group by operation
        """
        name = df + '_string_metadata.csv'
        for col in string_cols:
            query = "SELECT `{}` as col_value, count(*) as cnt FROM {} GROUP BY `{}`".format(col, df, col)
            x = self.spark.sql(query)
            x = x.withColumn("col_name", f.lit(col))
            x.coalesce(1).write.save(path = name, header= "true", mode = "append", format = "csv", sep = '^')

    def timeColWithinRange(self, minTime, maxTime):
        """
            Returns columns whose values are within given time range.
            :param minTime: time value in a python datetime object
            :param maxTime: time value in a python datetime object
            :return: A dataframe with date column names having overlapping range
        """
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
        """
            Returns columns whose values are within given number range.
            :param minNum: lower bound to range of type int, bigint, float, long
            :maxNum: upper bound to range of type int, bigint, float, long
            :return resultDf: A dataframe with date column names having overlapping range
        """
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
                    resultDf = currentTable.where(currentTable.min>minNum).\
                    where(currentTable.max<maxNum).select(currentTable.colName).withColumn("tableName", f.lit(each))
                    resultCreated = True
                else:
                    resultDf = resultDf.union(currentTable.\
                                              where(currentTable.min>minNum).where(currentTable.max<maxNum).\
                                              select(currentTable.colName).withColumn("tableName",f.lit(each)))
        return resultDf

    def getNumRange(self,colList):
        """
            Find the range of values of numerical data columns.
            :param colList: A list of column^tableName
            :return resultDF: dataframe with range lower and upper bound value of the column
        """
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
        """
            Find the range of values of timestamp data columns.
            :param colList: A list of column^tableName
            :return resultDf: dataframe with range lower and upper bound value of the column
        """
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
        """
            A function that compares the percentage overlap of two columns over a threshold.
            :param tableColName: A string having column^tableName
            :param threshold: A float value between 0 and 1 which determines the percentage
            overlap/intersection which the user expects
            :return resultDf: A dataframe having tableName, column name and itersection over union
        """
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
                        resultDf = currentTable.where\
                        (currentTable.colName == row["colName"]).\
                        select(currentTable.colName).withColumn("tableName", f.lit(each)).withColumn("iou", f.lit(iou))
                        resultCreated = True
                    else:
                        resultDf = resultDf.union(currentTable.where\
                                                  (currentTable.colName == row["colName"]).select(currentTable.colName).\
                                                  withColumn("tableName", f.lit(each)).withColumn("iou", f.lit(iou)))
        if resultDf == None:
            resultSchema = StructType([
                StructField("tableName", StringType(), True),
                StructField("colName", StringType(), True),
                StructField("iou", DoubleType(), True)])
            return self.spark.createDataFrame(self.sc.emptyRDD(), resultSchema)
        return resultDf.sort(f.desc("iou"))

    def returnIntersecWithinCols(self,colList):
        """
            Find the set of unique values common in two columns from different or same table.
            :param colList: A list of tableName^colName
            :return resultDF: A distinct set of values overlapping in both the columns
        """
        resultCreated = False
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


    def frequentVals(self, colList, topN):
        """
            Finds topN frequent column values of the input column.
            :param colList: A list of tableName^colName
            :return resultDF: column values and frequency of columns
        """
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
        """
            Returns column values that appears equal or less to the given threshold.
            :param colList: A list of tableName^colName
            :param percentage: A float value
            :return resultDF: column values and frequency of columns
        """
        result = []
        for each in colList:
            tableName, colName = each.split('^', 1)
            filename = tableName + '_string_metadata.csv'
            if self.fs.exists(self.sc._jvm.org.apache.hadoop.fs.Path(filename)):
                currentTable = self.spark.read.format('csv').options(header='true',inferschema='true', sep = '^').load(filename)
                newDf = currentTable.where(currentTable.col_name==colName)
                numRows = newDf.agg(f.sum('cnt').alias('aggregate')).first().aggregate

                # find the maximum number of outliers to fetch
                numOutliers = round(numRows * percentage)

                # sort the dataframe in ascending order of frequency
                newDf = newDf.sort(f.asc('cnt'))

                # check how many rows to fetch
                rows = newDf.rdd.collect()
                rowLim = 0
                aggSum = 0
                for row in rows:
                    if (aggSum + row['cnt']) < numOutliers:
                        aggSum += row['cnt']
                        rowLim += 1
                    else:
                        break
                # fetch the first "rowLim" rows
                newDf = newDf.limit(rowLim)
                result.append(newDf)

        for df in result:
            df.show()


    def colsWithAndWithout(self, colList, withList, withoutList):
        """
            Returns a list of column names that includes given words and
             excludes given words.
           :param colList: A list having string argumenet tableName^ColumnName
           :param withList: A list with of keywords
           :param withoutList: A list of keywords
           :output result : List of Columns that satify the conditon
        """
        resultCreated = False

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
                        if not resultCreated:
                            resultDf = currentTable.where(currentTable.col_name==colName)\
                            .select(currentTable.col_name).withColumn("table_name", f.lit(tableName))
                            resultCreated = True
                        else:
                            resultDf = resultDf.union(currentTable.where(currentTable.col_name==colName)\
                            .select(currentTable.col_name).withColumn("table_name", f.lit(tableName)))
        if not resultCreated:
            resultSchema = StructType([
                StructField("table_name", StringType(), True),
                StructField("col_name", StringType(), True)])
            return self.spark.createDataFrame(self.sc.emptyRDD(), resultSchema)
        else:
            resultDf = resultDf.dropDuplicates()
            return resultDf

    def getCardinality(self, colList):
        """
            Finds the number of distinct values in the given column(s).
            for categorical columns.
            :param colList: A list of tableName^colName
            :return resultDF: A dataframe having column names and number of distinct values
        """
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
        """
            Returns columns whose values are states, county, or city.
            :param df: A Spark Dataframe
            :param coltype: {'all', 'timestamp', 'string', 'boolean'}
            :return _col : List of columsn that satify coltype
        """
        # Clean up column names so that we can prevent future errors
        for colName, dtype in df.dtypes:
            if '.' in colName or '`' in colName or colName.strip() != colName:
                df = df.withColumnRenamed(colName, colName.strip().replace(".", "").replace("`", ""))

        time_cols, string_cols, bool_cols, num_cols = [], [], [], []

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
                    return string_cols
            elif(coltype == 'int' or coltype == 'Integer'):
                if(num_cols):
                    return string_cols
            elif(coltype == 'boolean' or coltype == 'Boolean'):
                if(bool_cols):
                    return bool_cols
            elif(coltype == 'time' or coltype == 'Time'):
                if(time_cols):
                    return time_cols
        else:
            if(string_cols == []):
                print("There are no string columns")
            if(num_cols != []):
                print("There are no Integer columns")

            if(bool_cols != []):
                print("There are no Boolean columns")

            if(time_cols != []):
                print("There are no Time columns")
            return time_cols, string_cols, bool_cols, num_cols

    def colsNameSimilarity(self, df, category = None, df2=None):
        """
            Compares column names from two different tables and returns
            pairs of column names with their similarity.
            :param df: A Spark Dataframe
            :param category: A string keyword to match
            :df2 : A second dataframe to match column names
            :return result_df : A dataframe having column_1, column_2, path similarity, levenshtein distance,soundex_equality
        """
        # Clean up column names so that we can prevent future errors
        for colName, dtype in df.dtypes:
            if '.' in colName or '`' in colName or colName.strip() != colName:
                df = df.withColumnRenamed(colName, colName.strip().replace(".", "", "_").replace("`", ""))
        if(df2 == None):
            result_df = pd.DataFrame(columns= ['Column_1','Path Similarity'])
            category_sys = wn.synsets(category)
            if(category_sys != []):
                cnt = 0
                # put column names into appropriate bin
                for colName, dtype in df.dtypes:
                    colName_ = colName.split("_")
                    score = []
                    for i in range(len(colName_)):
                        colName_sys = wn.synsets(colName_[i])
                        if(colName_sys != []):
                            score.append(colName_sys[0].path_similarity(category_sys[0]))
                    if(score != []):
                        score = max(score)
                    else:
                        score = 0
                    result_df.loc[cnt] = [colName, score]
                    cnt += 1
            else:
                print("Similarity cannot be calculated")
        else:
            for colName, dtype in df2.dtypes:
                if '.' in colName or '`' in colName or colName.strip() != colName:
                    df2 = df2.withColumnRenamed(colName, colName.strip().replace(".", "", "_").replace("`", ""))
            result_df = pd.DataFrame(columns= ['Column_1', 'Column_2','Path Similarity'])
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
                    score = [i for i in score if i!=None]
                    if(score != []):
                        score = max(score)
                    else:
                        score = 0
                    result_df.loc[cnt] = [colName1, colName2, score]
                    cnt += 1
        result_df = result_df[result_df['Path Similarity'] > 0.5]
        if(result_df.empty is not True):
            result_df = self.spark.createDataFrame(result_df)
            if(category is None):
                result_df = result_df.withColumn("levenshtein distance", f.levenshtein(result_df["Column_1"],\
                                                                                       result_df["Column_2"]))
                result_df = result_df.withColumn("soundex_equality", f.soundex(result_df["Column_1"]) ==\
                                                 f.soundex(result_df["Column_2"]))
            else:
                result_df = result_df.withColumn("levenshtein distance", \
                                                 f.levenshtein(result_df["Column_1"],f.lit(category)))
                result_df = result_df.withColumn("soundex_equality", f.soundex(result_df["Column_1"]) ==\
                                                 f.soundex(f.lit(category)))

        else:
            schema = StructType([
            StructField("Column_1", StringType(), True),
            StructField("Path Similarity", DoubleType(), True),
            StructField("levenshtein distance", DoubleType(), True),
            StructField("soundex_equality", DoubleType(), True),])
            result_df = self.spark.createDataFrame(self.sc.emptyRDD(), schema=schema)
        return result_df


    def getColsofCategory(self, tableName, colList, category):
        """
            Returns columns whose values are states, county, or city.
            : param tableName: Name of a table
            : param colList: A list of column names
            : param category: 'State_full', 'County', 'State_short', 'City'
            : return resut_df: A dataframe that consists of tableName, colName,
             category, and IsSubset
        """
        result_df = pd.DataFrame(index = colList, columns = ["tableName","colName", "category", "IsSubset"])
        if(category in ['State_full', 'County', 'State_short', 'City']):
            if(tableName in self.tableNames and "category" in self.tableNames):
                for i in colList:
                    cols = ["category^"+category, tableName+"^"+i]
                    result_insec = self.returnIntersecWithinCols(cols)
                    result_insec = result_insec.filter(result_insec['col_value'] != 'null')
                    if(result_insec.count() != 0):
                        print("Column values are a subset of {}".format(category))
                        result_df.loc[i] = [tableName,i,category, True]
                    else:
                        print("Column values are not a subset of {}".format(category))
                        result_df.loc[i] = [tableName,i,category, False]
        else:
            print("Category does not exist. Data cannot be validated")
        result_df = self.spark.createDataFrame(result_df)
        return result_df

    def countNullValues(self, table_col_list):
        """
            Returns the number of null values in each column.
            :param table_col_list: A nest list having list of [table, column]
            :return result_df: A dataframe Table name, Column name and Null Values
        """
        result_df = pd.DataFrame(columns = ['Table name','column name','Null Values'])
        cnt = 0
        for i in table_col_list:
            table, column = i[0], i[1]
            filename = table+'_string_metadata.csv'
            if self.fs.exists(self.sc._jvm.org.apache.hadoop.fs.Path(filename)):
                currentTable = self.spark.read.format('csv').\
                options(header='true',inferschema='true', sep = '^').load(filename)
                currentTable = currentTable.filter(currentTable["col_name"] == column)
                x = currentTable.filter(currentTable["col_value"].isNull())
                try:
                    count_val = x.select('cnt').collect()[0][0]
                    #print(count_val)
                    result_df.loc[cnt] = [table, column, count_val]

                except:
                    result_df.loc[cnt] = [table, column, 0]
                cnt += 1
        if(result_df.empty is False):
            result_df = self.spark.createDataFrame(result_df)
        else:
            schema = StructType([
            StructField("Table name", StringType(), True),
            StructField("column name", StringType(), True),
            StructField("Null Values", DoubleType(), True)])
            result_df = self.spark.createDataFrame(self.sc.emptyRDD(), schema=schema)
        return result_df


    def returnUnionWithinCols(self,colList):
        """
            Find the union set of unique column values present in input columns.
            :param colList: A list of tableName^colName
            :return resultDF: A distinct set of values in both the columns
        """
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
        resultDf = resultDf.dropDuplicates()
        return resultDf
