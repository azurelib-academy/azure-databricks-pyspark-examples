# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Groupby Count in Azure Databricks

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
    ("XC70","USA",2002),
    ("NV2500","Europe",2004),
    ("Savana 2500","USA",2002),
    ("Dakota","Europe",2002),
    ("LeSabre","Japan",2002),
    ("Cooper","Japan",2004),
    ("929","Europe",2004),
    ("Eldorado","Europe",2002),
    ("09-May","Japan",2002),
    ("Daewoo Kalos","USA",2004)
]

df = spark.createDataFrame(data, schema=["model","origin","release_year"])
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### b) Create PySpark DataFrame by reading files

# COMMAND ----------

# replace the file_paths with the source file location which you have downloaded.

df_2 = spark.read.format("csv").option("header", True).load(file_path)
df_2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Note: Here, I will be using the manually created dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1) Groupby count on Single count

# COMMAND ----------

df.groupBy("origin").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2) Groupby count on multiple count

# COMMAND ----------

df.groupBy("origin","release_year").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3) GroupBy Count using SQL expression

# COMMAND ----------

df.createOrReplaceTempView("cars")

# 1. Single column
spark.sql('''
SELECT origin, count(1) AS count FROM cars
GROUP BY origin
''').show()

# 2. Multiple column
spark.sql('''
SELECT origin, count(1) AS count FROM cars
GROUP BY origin, release_year
''').show()