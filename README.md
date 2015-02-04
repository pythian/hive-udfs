hive-udfs
=========

Collection of Hive UDFs


Building and testing
====================
Check out the code and run 

1. To run tests: ```sbt test```
2. To assemble a jar: ```sbt assembly```


CountBusinessDays, CountSaturdays, CountSundays
===============================================

These UDFs count the number of full days between two given dates exclusive. For example the number of full business
days between Monday and Wednesday would be 1. 

**Usage**

```sql
ADD JAR hdfs:///user/hive/udfs/pythian-hive-udfs-assembly-0.1.jar;
CREATE TEMPORARY FUNCTION count_business_days AS 'com.pythian.hive.udf.CountBusinessDays';
CREATE TEMPORARY FUNCTION count_saturdays AS 'com.pythian.udf.hive.CountSaturdays';
CREATE TEMPORARY FUNCTION count_sundays AS 'com.pythian.udf.hive.CountSundays';

CREATE TABLE business_days_example(
        start_date STRING,
        end_date STRING,
        description STRING)
    COMMENT 'This is to test CountBusinessDays UDFs'
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH 'demo/business_days_example.txt' OVERWRITE INTO TABLE business_days_example;

SELECT count_business_days(UNIX_TIMESTAMP(start_date), UNIX_TIMESTAMP(end_date)) FROM business_days_example;
```

JsonSplit
=========

The split UDF accepts a single JSON string containing only an array. In the Hive CLI:

```sql
ADD JAR target/JsonSplit-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION json_split AS 'com.pythian.hive.udf.JsonSplitGenericUDF';

CREATE TABLE json_example (json string);
LOAD DATA LOCAL INPATH 'demo/split_example.json' INTO TABLE json_example;

SELECT ex.* FROM json_example LATERAL VIEW explode(json_split(json_example.json)) ex;
```

```json_split``` converts the string to the following array of structs, which are exploded into individual records: 

```json
[
  {
    row_id:1, 
    json_string:'1' 
  },
  { 
    row_id:2, 
    json_string:'2' 
  }, 
  {
    row_id:3, 
    json_string:'3' 
  }
]
```

You can access the JSON string for the element with the ```json_string``` attribute. The ```json_string``` can be any arbitrary JSON string, including another array or a nested object. ```row_id``` is the position in the array.


JsonMap
=======

The map UDF accepts a flat JSON object (only integer and string values, no arrays or maps) and converts it into a Hive map. The elements of the map don't have to be defined until query-time, and can be accessed with the square bracket syntax ['key'].

```sql
ADD JAR target/JsonSplit-1.0-SNAPSHOT.jar;
CREATE TEMPORARY FUNCTION json_map as 'com.pythian.hive.udf.JsonMapGenericUDF';

CREATE TABLE json_map_example (json string);
LOAD DATA LOCAL INPATH 'demo/map_example.json' INTO TABLE json_map_example;

SELECT json_map(json)['x'] FROM json_map_example LATERAL VIEW explode(array(json_map_example.json)) ex;
```

The above converts the JSON string to a map, then pulls out the value for each record's key 'x'.

Contribute new UDFs
===================

1. Write your UDF in Scala or Java
2. Update the build.sbt with your dependencies
3. Add source and tests to respective directories
4. Update README with usage instructions for your UDF

