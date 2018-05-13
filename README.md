# TableCollections
## Introduction
Table Collections includes 11 queries for exploring data in various ways on single or multiple datasets even when the contents are unclear to the user. Metadata is generated only once when a dataset is loaded. When a query is called, it runs on pre-generated metadata. Columns within same table or columns from multiple tables can be compared. The queries have been tested on multiple sized data from NYC Open Data. (https://opendata.cityofnewyork.us/)

## Usage
    tc = TableCollections(spark, sc)
    tc.register(openTable, "open") # Create Metadata for New Table
    tc.add_registered_table_name("parking") # Load Generated Metadata
    tc.numColWithinRange(-76, 70).show() # Use Class Method
    tc.query(“colsWithinRange -76, -70”).show() # Use Custom Query Language

## Running the Program
Test results on selected NYC Open Data samples:

    tests/Test_all.ipynb

To run the program:

    ./submit.sh

## Queries

### All Types

* `countNullValues` : Returns the number of null values in each column
    * _query._ countNullValues “tablename”
* `colNameSimilarity` : Compares column names from two different tables and returns pairs of column names with their similarity
    * _query._ colNameSimilarity “table1” “table2”
* `getColsOfDatatype` : Returns columns whose datatype matches the input datatype
    * _query._ getColsOfDatatype “string”

### Numerical Types

* `colsWithinRange` : Returns columns whose values are within given range
    * _ex._ Find nyc longitude columns where their values are in (-76,-70)
    * _query._ colsWithinRange -76, -70​
* `similarCols` : Returns columns who are similar to the given column. Similarity is
defined as intersection over union greater than threshold
    * _ex._ Find all columns whose values are similar to a latitude column
    * _query._ similarCols nyc_2n4x_d97d^Latitude where iou >= 0.001​
* `getRange` : Returns saved metadata for given columns
    * _ex._ Find min and max values of the result of SimilarCols
    * _query._ getRange "nyc_2n4x_d97d^Latitude", "nyc_29km_avyc^latitude", "nyc_35f6_8qd2^Borough"

### Categorical Types
* `intersectionWithinCols` : Returns column values that are present in every input columns
    * _ex._ Check if a person appears in multiple violation records
    * _query._ intersectionWithinCols [“column1”, “column2”]
* `getCardinality` : Returns the number of unique values in the column
    * _ex._ Check how many types of violations were reported
    * _query._ getCardinality [“column1”, “column2”]
* `colsWithAndWithout`: Returns a list of column names that includes given words and excludes given words
    * _ex._ Check if ‘Brooklyn’ was misclassified as a city name
    * _query._ colsWithAndWithout [“Brooklyn”], [“Manhattan”, “Queens”]
* `frequentVals` : Returns a list of column values that are top N frequent in the decreasing order of frequency
    * _ex._ Check top 10 types of premises where complaints were reported
    * _query._ frequentVals [“column1”, “column2”], 10
* `getColsofCategory`: Returns columns whose values are states, county, or city
    * _query._ getColsofCategory “State_short”
* `returnOutliers`: Returns column values that appears equal or less to the given threshold
	* _query._ returnOutliers [“column1”]
