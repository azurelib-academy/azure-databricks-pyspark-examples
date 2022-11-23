# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark Time Difference in Azure Databricks

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
    ("17:54:02.000","2022-09-15 22:47:05","5:40:01.000 PM"),
    ("15:13:55.000","2022-05-21 02:30:12","3:50:43.000 AM"),
    ("10:30:55.000","2022-10-11 11:12:11","3:33:13.000 PM"),
    ("12:28:13.000","2022-06-14 06:11:39","12:22:24.000 AM"),
    ("12:38:58.000","2022-06-12 18:19:34","4:23:13.000 PM")
]

df = spark.createDataFrame(data, schema=["time_1","date_time_1","date_time_2"])
df.printSchema()
df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### b) Create PySpark DataFrame by reading files

# COMMAND ----------

file_path = "/mnt/practice/time_difference.csv"
# replace the file_path with the source file location which you have downloaded.

df_2 = spark.read.format("csv").option("inferSchema", True).option("header", True).load(file_path)
df_2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Note: Here, I will be using the manually created dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1) Time difference by casting column of Date Time to LongType

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, current_timestamp, col

# 1. Converting the date_time_1 column to timestampe format
df1 = df.select(
    to_timestamp("date_time_1").alias("date_time"),
    current_timestamp().alias("curr_time")
)
df1.printSchema()
df1.show(truncate=False)

# 2. Converting timestamp to long type
df1 = df1.select(col("date_time").cast("long"), col("curr_time").cast("long"))

# 3. Subtract casted columns
df1 = df1.withColumn("diff_sec", col("curr_time")-col("date_time"))
df1.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2) Time difference by converting Date Time to Unix Timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, unix_timestamp, col

# Preparing DatFrame
df2 = df.select(
    col("date_time_1").alias("date_time"),
    current_timestamp().alias("curr_time")
)
df2.printSchema()
df2.show(truncate=False)

# 1. Converting timestamp to unixtime
df2 = df2.select(
    unix_timestamp("date_time").alias("date_time"),
    unix_timestamp("curr_time").alias("curr_time"))

# 2. Subtract times
df2 = df2.withColumn("diff_sec", col("curr_time")-col("date_time"))
df2.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3) Time differnce in minutes

# COMMAND ----------

from pyspark.sql.functions import col, round

df3 = df1 \
.withColumn("diff_min", round(col("diff_sec") / 60)) \
.drop("diff_sec")

df3.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4) Time differnce in hours

# COMMAND ----------

from pyspark.sql.functions import col, round

df4 = df1 \
.withColumn("diff_hrs", round(col("diff_sec") / 3600)) \
.drop("diff_sec")

df4.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 5) Time difference by parsing Time

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, current_timestamp, col

# 1. Converting the time_1 column to timestampe format
df5 = df.select(
    to_timestamp("time_1").alias("date_time"),
    current_timestamp().alias("curr_time")
)

# 2. Converting timestamp to long type
df5 = df5.select(col("date_time").cast("long"), col("curr_time").cast("long"))

# 3. Subtract times
df5 = df5.withColumn("diff_sec", col("curr_time")-col("date_time"))
df5.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 6) Time difference by parsing  DateTime is not in PySpark default format

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, current_timestamp, col

# 1. Converting the time_1 column to timestampe format
df6 = df.select(
    to_timestamp("date_time_2", "h:mm:ss.SSS a").alias("date_time"),
    current_timestamp().alias("curr_time")
)

# 2. Converting timestamp to long type
df6 = df6.select(col("date_time").cast("long"), col("curr_time").cast("long"))

# 3. Subtract times
df6 = df6.withColumn("diff_sec", col("curr_time")-col("date_time"))
df6.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 7) Time difference using Spark SQL

# COMMAND ----------

small_data = [("2020-01-01 00:00:00", "2021-12-31 11:59:59")]
df7 = spark.createDataFrame(small_data, schema=["from","to"])
df7.createOrReplaceTempView("sql_date_time")

spark.sql('''
SELECT
    unix_timestamp(`to`) - unix_timestamp(`from`) AS diff_sec,
    ROUND((unix_timestamp(`to`) - unix_timestamp(`from`)) / 60) AS diff_min,
    ROUND((unix_timestamp(`to`) - unix_timestamp(`from`)) / 3600) AS diff_hrs
FROM `sql_date_time`
''').show(truncate=False)