# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark Regexpr_Replace() Function in Azure Databricks

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
    (1, "Male","2022-10-25"),
    (2, "Female","2021/12/17"),
    (3, "Female","18.11.2021")
]

df = spark.createDataFrame(data, schema=["id","gender","dob"])
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### b) Create PySpark DataFrame by reading files

# COMMAND ----------

file_path = "/mnt/practice/regexp_data.csv"
# replace the file_path with the source file location which you have downloaded.

df_2 = spark.read.format("csv").option("header", True).load(file_path)
df_2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Note: Here, I will be using the manually created dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Replacing column values

# COMMAND ----------

from pyspark.sql.functions import regexp_replace

# 1. regexp_replace with select
df.select("gender", regexp_replace("gender", "Male", "M").alias("male_replace")).show()

# 2. regexp_replace with withColumn
df.withColumn("female_replace", regexp_replace("gender", "Female", "Fm"))\
.select("gender", "female_replace").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Replacing column values conditionally

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, when, col

df.select("gender", 
          when(col("gender").startswith("M"), regexp_replace("gender", "Male", "M")) \
          .when(col("gender").startswith("F"), regexp_replace("gender", "Female", "Fm")) \
          .otherwise(col("gender")).alias("regex_conditional")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Replacing column values wit regex pattern

# COMMAND ----------

from pyspark.sql.functions import regexp_replace

df.select("dob", regexp_replace("dob", "[/.]", "-").alias("mod_dob")).show()