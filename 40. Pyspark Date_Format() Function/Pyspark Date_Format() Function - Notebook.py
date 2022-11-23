# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark Date_Format() Function in Azure Databricks

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
    ("2022-08-02","29-01-2022","2022-04-03 21:08:37"),
    ("2022-03-28","13-10-2022","2022-10-07 00:07:36"),
    ("2022-02-17","11-11-2021","2022-05-06 13:06:57"),
    ("2022-09-08","07-10-2022","2022-08-26 17:09:36"),
    ("2022-09-04","22-07-2022","2022-09-23 22:58:26")
]

columns = ["pyspark_date","other_date","pyspark_timestamp"]
df = spark.createDataFrame(data, schema=columns)
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### b) Create PySpark DataFrame by reading files

# COMMAND ----------

file_path = "/mnt/practice/dateformat.csv"
# replace the file_path with the source file location which you have downloaded.

df_2 = spark.read.format("csv").option("header", True).load(file_path)
df_2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Note: Here, I will be using the manually created dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Using DataFrame

# COMMAND ----------

from pyspark.sql.functions import date_format

# 1. with select
df.select("pyspark_date", 
          date_format("pyspark_date", "MMM-yyyy").alias("fmt_date"),
          "pyspark_timestamp", 
          date_format("pyspark_timestamp", "dd-MMM-yyyy").alias("fmt_time"),
         ).show()

# 2. withColumn
df \
.withColumn("fmt_date", date_format("pyspark_date", "dd/MMM/yyyy")) \
.withColumn("fmt_time", date_format("pyspark_timestamp", "dd/MMM")) \
.select("pyspark_date", "fmt_date", "pyspark_timestamp", "fmt_time") \
.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Using SQL expression

# COMMAND ----------

df.createOrReplaceTempView("datetime")

spark.sql("""
SELECT
    pyspark_date,
    date_format(pyspark_date, 'dd-MM-yyyy') AS fmt_date,
    pyspark_timestamp,
    date_format(pyspark_timestamp, 'dd-MMM-yyyy') AS fmt_time
FROM datetime
""").show()


# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Using date_format() with other date/time format

# COMMAND ----------

from pyspark.sql.functions import date_format

df.select("other_date", date_format("other_date", "MMM-yyyy").alias("fmt_date")).show()