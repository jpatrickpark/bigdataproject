import sys
from pyspark import SparkContext
from csv import reader
from pyspark.sql import SparkSession, Row, functions as f
<<<<<<< HEAD
from pyspark.sql.functions import udf
from datetime import datetime,date,timedelta
from pyspark.sql.types import StringType

=======
from pyspark.sql.functions import udf, lit
from datetime import datetime,date,timedelta
from pyspark.sql.types import StringType


>>>>>>> aa99db7ce4693e39070bb5e1cb5227a518a14b2b
#register the dataset
def register(file_path, name):
	name_str = name
	df = spark.read.format('csv').options(header = 'true',inferschema='true').load(file_path)
	df.createOrReplaceTempView(name)
	string_cols = []
	int_cols = []
	timestamp = []
	for colName, dtype in df.dtypes:
		if dtype == 'string':
			string_cols.append(colName)
		elif dtype == 'timestamp':
			timestamp.append(colName)
	string_metadata(name, string_cols, name_str)

#finding the string metadata for all columns
def string_metadata(df, string_cols, name):
	name = name+'_string_metadata.csv'
	for col in string_cols:
		query = "SELECT {} as col_value, count(*) as cnt FROM {} GROUP BY {}".format(col, df, col) 
		x = spark.sql(query)
<<<<<<< HEAD
		y = x.select(f.format_string(col+'\t%.2f, %.2f', x.col_value, x.cnt))
		x.select(f.format_string(col+', %s, %d', x.col_value, x.cnt)).write.mode("append").save(name,format="text")
=======
		x = x.withColumn("col_name", lit(col))
		x.coalesce(1).write.save(path = name, header= "true", mode = "append", format = "com.databricks.spark.csv", sep = '^')
>>>>>>> aa99db7ce4693e39070bb5e1cb5227a518a14b2b

if __name__ == "__main__":
	spark = spark = SparkSession.builder.appName("task7-sql").config("spark.some.config.option", "some-value").getOrCreate()
	name = sys.argv[2]
	file_path = sys.argv[1]
	register(file_path, name)
<<<<<<< HEAD




=======
>>>>>>> aa99db7ce4693e39070bb5e1cb5227a518a14b2b
