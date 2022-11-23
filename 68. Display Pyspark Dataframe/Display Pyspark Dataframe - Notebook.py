# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Display Pyspark Dataframe in Azure Databricks

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
    (1,"Nessie","Kesten","Physicians Total Care, Inc."),
    (2,"Giordano","Loseke","Concept Laboratories, Inc."),
    (3,"Issie","MacLeese","Conopco Inc. d/b/a Unilever"),
    (4,"Rozina","Fursse","Acura Pharmaceuticals, Inc."),
    (5,"Robers","Doody","Aqua Pharmaceuticals")
]

df = spark.createDataFrame(data, schema=["id", "first_name", "last_name", "company"])
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### b) Create PySpark DataFrame by reading files

# COMMAND ----------

file_path = "/mnt/practice/medical_employees.csv"
# replace the file_path with the source file location which you have downloaded.

df_2 = spark.read.format("csv").option("header", True).load(file_path)
df_2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Note: Here, I will be using the manually created dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Controlling number of rows

# COMMAND ----------

# 1. Show limits 20 record by default
df.show(truncate=False)

# 2. Changing limit value
df.show(n=3, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Priting each record vertically

# COMMAND ----------

df.show(n=2, vertical=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. How to use truncate parameter

# COMMAND ----------

# 1. With truncate
df.show(n=5, truncate=True)

# 2. Without truncate
df.show(n=5, truncate=False)

# 3. Truncate further
df.show(n=5, truncate=10)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4. Spliting the column with you have multiple delimiter

# COMMAND ----------

from pyspark.sql.functions import split, size

df.select(
    split("names_3", "[, |]").alias("multiple_delimiter"),
    size(split("names_3", "[, |]")).alias("length"),
).show(truncate=False)