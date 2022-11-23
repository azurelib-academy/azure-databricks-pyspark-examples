# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Total And Average Of Column In Pyspark in Azure Databricks

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
    ("chevrolet vega 2300","USA",None,90,28.0,"1970-01-01"),
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
df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### b) Create PySpark DataFrame by reading files

# COMMAND ----------

file_path = "/mnt/formulaoneadls/practice/cars_stat.csv"
# replace the file_path with the source file location which you have downloaded.

df_2 = spark.read.format("csv").option("inferSchema", True).option("header", True).load(file_path)
df_2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Note: Here, I will be using the manually created dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Using sum() & avg() on selected column

# COMMAND ----------

from pyspark.sql.functions import sum, avg

df.select(
    sum("acceleration").alias("total_acc"),
    avg("miles_per_gallon").alias("avg_mileage")
).show()

# Note: Will not conside null/None values

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Using sum() & avg() by grouping columns

# COMMAND ----------

from pyspark.sql.functions import sum, avg

df.groupBy("origin") \
.agg(
    sum("acceleration").alias("total_acc"),
    avg("miles_per_gallon").alias("avg_mileage")
).show()

# Note: Will not conside null/None values