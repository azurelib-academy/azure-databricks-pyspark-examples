# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark Substring() Function in Azure Databricks

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
    ("Boehm, Volkman and Homenick","2022-08-06"),
    ("Kshlerin Inc","2009-03-30"),
    ("Kreiger, Weber and Brakus","2005-06-07"),
    ("Adams-Lowe","2013-02-16"),
    ("O'Conner-Herman","2022-07-20")
]

df = spark.createDataFrame(data, schema=["name","started"])
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### b) Create PySpark DataFrame by reading files

# COMMAND ----------

file_path = "/mnt/practice/name_list.json"
# replace the file_path with the source file location which you have downloaded.

df_2 = spark.read.format("csv").option("header", True).load(file_path)
df_2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Note: Here, I will be using the manually created dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Substring() using dataframe

# COMMAND ----------

from pyspark.sql.functions import substring

# 1. with select
df.select(
    "*",
    substring("started", 1, 4).alias("year"),
    substring("started", 6, 2).alias("month"),
    substring("started", 9, 2).alias("date")
).show(truncate=False)


# 2. with withColumn
df \
.withColumn("year", substring("started", 1, 4)) \
.withColumn("month", substring("started", 6, 2)) \
.withColumn("date", substring("started", 9, 2)) \
.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Substring on SQL operation

# COMMAND ----------

df.createOrReplaceTempView("company")

spark.sql("""
SELECT
    name,
    SUBSTRING(started, 1,4) AS year,
    SUBSTRING(started, 6,2) AS month,
    SUBSTRING(started, 9,2) AS date
FROM company""").show(truncate=False)

# COMMAND ----------

.