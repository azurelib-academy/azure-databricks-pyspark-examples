# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark Aggregation Functions in Azure Databricks

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Gentle reminder: 
# MAGIC In Databricks,
# MAGIC   - sparkSession made available as spark
# MAGIC   - sparkContext made available as sc
# MAGIC   
# MAGIC In case, you want to create it manually, use the below code.

# COMMAND ----------

from pyspark.sql.session import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("azurelib.com") \
    .getOrCreate()

sc = spark.sparkContext

# COMMAND ----------

# MAGIC %md
# MAGIC ##### a) Create manual PySpark DataFrame

# COMMAND ----------

data = [
    ("chevrolet vega 2300","USA",15.5,90,28.0,"1970-01-01"),
    ("chevrolet vega 2300","USA",15.5,90,28.0,"1970-01-01"),
    ("toyota corona","Japan",14.0,95,25.0,"1970-01-01"),
    ("ford pinto","USA",19.0,75,25.0,"1971-01-01"),
    ("amc gremlin","USA",13.0,100,19.0,"1971-01-01"),
    ("plymouth satellite custom","USA",15.5,105,16.0,"1971-01-01"),
    ("datsun 510 (sw)","Japan",17.0,92,28.0,"1972-01-01"),
    ("toyouta corona mark ii (sw)","Japan",14.5,97,23.0,"1972-01-01"),
    ("dodge colt (sw)","USA",15.0,80,28.0,"1972-01-01"),
    ("toyota corolla 1600 (sw)","Japan",16.5,88,27.0,"1972-01-01")
]

columns = ["name","origin","acceleration","horse_power","miles_per_gallon","year"]
df = spark.createDataFrame(data, schema=columns)
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### b) Create PySpark DataFrame by reading files

# COMMAND ----------

file_path = "/mnt/formulaoneadls/practice/cars_stats.csv"
# replace the file_path with the source file location which you have downloaded.

df_2 = spark.read.format("csv").option("inferSchema", True).option("header", True).load(file_path)
df_2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Note: Here, I will be using the manually created dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Single column aggregation

# COMMAND ----------

from pyspark.sql.functions import count, sum, avg

# 1. Single column aggregation
df.select(count("name")).show()

# 2. Multiple column aggregation
df.select(min("horse_power"), max("horse_power")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Groupby column aggregation

# COMMAND ----------

from pyspark.sql.functions import *

# 1. Single column grouping aggregation
df.groupBy("origin").count().show()

# 2. Multiple column grouping aggregation
df.groupBy("year", "origin").max("horse_power").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Perform multiple aggregation using agg() function

# COMMAND ----------

from pyspark.sql.functions import min, avg, max

df.groupBy("origin").agg(
    min("horse_power"),
    avg("horse_power"),
    max("horse_power")    
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4. Rename aggregated columns using alias()

# COMMAND ----------

from pyspark.sql.functions import min, avg, max

df.groupBy("origin").agg(
    min("horse_power").alias("min_hp"),
    avg("horse_power").alias("avg_hp"),
    max("horse_power").alias("max_hp")
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Various In-built Aggregation Functions in PySpark

# COMMAND ----------

from pyspark.sql.functions import *

# 1. count, countDistinct
'''
count() returns the number of elements in a column.
countDistinct() returns the number of distinct elements in a columns
'''
df.select(count("name"), countDistinct("year", "acceleration")).show()

# 2. sum, sumDistinct
'''
sum() returns the sum of all values in a column.
sumDistinct() returns the sum of all distinct values in a column
'''
df.select(sum("horse_power"), sumDistinct("horse_power")).show()

# 3. stddev, stddev_samp, stddev_pop
'''
stddev() alias for stddev_samp.
stddev_samp() returns the sample standard deviation of values in a column.
stddev_pop() returns the population standard deviation of the values in a column.
'''
df.select(stddev("horse_power"), stddev_samp("horse_power"), stddev_pop("horse_power")).show()

# 4. avg, mean
'''
avg() returns the average of values in the input column.
mean() is an alias of avg() function, works exactly like avg().
'''
df.select(avg("acceleration"), mean("acceleration")).show()

# 5. first, last
'''
first() returns the first value in a column.
last() returns the last value in a column.
'''
df.select(first("horse_power"), last("horse_power")).show()

# 6. min, max
'''
min() returns the minimum value in a column.
max() returns the maximum value in a column.
'''
df.select(min("horse_power"), max("horse_power")).show()

# 7. variance, variance_samp, variance_pop
'''
variance() alias for var_samp
var_samp() returns the unbiased variance of the values in a column.
var_pop() returns the population variance of the values in a column.
'''
df.select(variance("horse_power"), var_samp("horse_power"), var_pop("horse_power")).show()

# 8. collect_list, collect_set
'''
collect_list() returns all values from an input column with duplicates.
collect_set() returns all values from an input column with duplicate values eliminated.
'''
df.select(collect_list("horse_power"), collect_set("horse_power")).show(truncate=False)