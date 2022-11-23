# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark Sql Timestamp Functions in Azure Databricks

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
    ("11:58:12.000 PM","12:20:06.000","2022-06-28 18:51:38"),
    ("10:59:42.000 PM","12:26:04.000","2021-12-23 07:51:28"),
    ("5:54:46.000 PM","17:47:13.000","2022-04-24 16:54:36"),
    ("6:16:26.000 PM","2:49:42.000","2021-11-15 17:22:32"),
    ("12:28:07.000 PM","19:01:51.000","2022-08-23 07:18:19"),
    ("6:55:01.000 AM","22:09:54.000","2022-02-23 01:00:06"),
    ("12:50:06.000 PM","4:17:42.000","2022-04-02 17:34:48"),
    ("4:16:39.000 PM","19:30:41.000","2021-10-31 00:24:51"),
    ("10:05:03.000 PM","15:59:34.000","2022-08-06 23:37:36"),
    ("9:44:34.000 PM","4:14:07.000","2022-07-19 04:57:58")
]

columns = ["time_1","time_2","date_time"]
df = spark.createDataFrame(data, schema=columns)
df.printSchema()
df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### b) Create PySpark DataFrame by reading files

# COMMAND ----------

file_path = "/mnt/practice/time.csv"
# replace the file_path with the source file location which you have downloaded.

df_2 = spark.read.format("csv").option("inferSchema", True).option("header", True).load(file_path)
df_2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Note: Here, I will be using the manually created dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Time function

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, hour, minute, second

# 1. current time
df \
.withColumn("current_time", current_timestamp()) \
.select("current_time") \
.show(5, truncate=False)

# 2. Extract time function
df \
.withColumn("Hour", hour("time_2")) \
.withColumn("Minute", minute("time_2")) \
.withColumn("Second", second("time_2")) \
.select("time_2", "Hour", "Minute", "Second") \
.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Convert 12hr to 24hr front and back

# COMMAND ----------

from pyspark.sql.functions import unix_timestamp, from_unixtime

# 1. convert 12hr to 24hr

fmt_df = df \
.withColumn("unix_time", unix_timestamp("time_1", "h:mm:ss.SSS a")) \
.withColumn("from_unix", from_unixtime("unix_time", "k:mm:ss.SSS")) \
.selectExpr("time_1 as 12hr", "unix_time", "from_unix as 24hr")

fmt_df.show(5)

# 2. convert 12hr to 24hr

fmt_df.select("24hr") \
.withColumn("unix_time", unix_timestamp("24hr", "k:mm:ss.SSS")) \
.withColumn("12hr", from_unixtime("unix_time", "h:mm:ss.SSS a")) \
.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Convert timestamp column of StringType to TimeStamp

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, col

# String
df.select("time_2").printSchema()

# Various method to convert string to date format
# Method 1:
df.withColumn("time_2", to_timestamp("time_2")) \
.select("time_2").printSchema()

# Method 2:
df.withColumn("time_2", col("time_2").cast("Timestamp")).select("time_2").printSchema()

# Method 3:
df.selectExpr("CAST(time_2 as TIMESTAMP)").printSchema()