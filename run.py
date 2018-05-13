# to be run with submit.sh
from pyspark.sql import SparkSession
from pyspark.sql import Row
import sys
import string
from csv import reader
from functools import reduce
from pyspark.sql import functions as f
from collections import defaultdict
import datetime
sys.path.insert(0, 'src')
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
    # The following line assumes the file is present in hdfs, as it would be if ran by submit.sh
    category = spark.read.format('csv').options(header='true',inferschema='true').load('us_cities_states_counties.csv')

    tc = TableCollections(spark, sc)

    # Register the tables (create metadata if does not exists)
    tc.register(openTable, "open")
    tc.register(parkingTable, "parking")
    # The following line must be run in order to run some methods
    tc.register(category, "category")

    
    ## Returns names of time columns within a range of dates 
    tc.timeColWithinRange(datetime.datetime(1994,1,1), datetime.datetime(2018,5,1)).show()

    ## Returns name of Numerical columns within a range of values
    tc.numColWithinRange(-1000000, 10000000).show()

    ## Returns range of value(minimum and maximum) that a column has in particular table
    colList= ["parking^violation_location","parking^violation_precinct", "open^reduction_amount", "open^amount_due"]
    tc.getNumRange(colList).show()

    ## Returns time range of timestame columns
    colList= ["parking^issue_date"]
    tc.getTimeRange(colList).show()

    ## Return column names having intersection over union greater than a threshold
    tc.getSimilarNumCols("parking^violation_location", threshold=0.1).show()

    ## Returns intersecting values in two columns of different or same tables
    columnsA = ["parking^plate_id", "open^plate"]
    tc.returnIntersecWithinCols(columnsA).show()

    ## Return the top n most frequent values in different tables and columns
    tc.frequentVals(columnsA, 10).show()

    ## Return outlier based on frequency beyond a theshold in a column in specified table
    tc.returnOutliers(columnsA, 0.02)

    ## Finds the number of unique values in a columns in specified table
    tc.getCardinality(columnsA).show()

    ## Column names in a table with and without some keywords
    columnsB = ["parking^vehicle_color", "parking^vehicle_make"]
    tc.colsWithAndWithout(columnsB, ["FORD"], ["GREEN"]) ## returns parking^vehicle_make
    tc.colsWithAndWithout(columnsB, ["FORD", "TOYOTA"], ["GREEN"]) ## returns empty result ("TOYOT" is in the column but not "TOYOTA")


    ## Returns columns of specified datatype in a dataframe
    tc.getColsOfDatatype(openTable, 'string')

    ## Find if a column is of type State(full, short), County and City for United States
    tc.getColsofCategory('parking',['registration_state'],category= 'State_full').show()
    tc.getColsofCategory('parking',['registration_state'],category= 'State_short').show()

    ## Returns number of null values in specified table and columns
    tc.countNullValues([['parking', 'violation_county']]).show()

    ## Return Union values in two columns of same or different tables
    colList= ["parking^plate_id","open^plate"]
    tc.returnUnionWithinCols(colList).show()

    import nltk # You can install nltk using pyvenv
    nltk.download('wordnet')
    # Returns similarity measures for columns having path similarity greater than 0.5 for different dataframe or a keyword category
    tc.colsNameSimilarity(openTable, category = None, df2=parkingTable).show()
    tc.colsNameSimilarity(openTable, category = 'state', df2=None).show()
    tc.colsNameSimilarity(parkingTable, category = 'state', df2=None).show()
