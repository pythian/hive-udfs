# hive-udfs
Collection of Hive UDFs

## CountBusinessDays, CountSaturdays, CountSundays
These UDFs count the number of full days between two given dates exclusive. For example the number of full business
days between Monday and Wednesday would be 1. 

**Usage**

```sql
ADD JAR hdfs:///user/hive/udfs/pythian-hive-udfs-assembly-0.1.jar;
CREATE TEMPORARY FUNCTION count_business_days AS 'com.pythian.udf.CountBusinessDays';
CREATE TEMPORARY FUNCTION count_saturdays AS 'com.pythian.udf.CountSaturdays';
CREATE TEMPORARY FUNCTION count_sundays AS 'com.pythian.udf.CountSundays';

SELECT count_business_days(UNIX_TIMESTAMP(start_date), UNIX_TIMESTAMP(end_date)) FROM some_table; 
```

## Building and testing
1. To run tests: ```sbt test```
2. To assembly a jar: ```sbt assembly```