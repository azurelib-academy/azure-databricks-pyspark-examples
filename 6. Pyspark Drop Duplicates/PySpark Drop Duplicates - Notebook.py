# Databricks notebook source
# MAGIC %md
# MAGIC #### PySpark drop duplicates in Azure Databricks

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
    ("Meenu", "Teacher", "Trichy"),
    ("Meenu", "Teacher", "Trichy"),
    ("Chandru", "Electrician", "Salem"),
    ("Chandru", "Electrician", "Chennai")
]

df = spark.createDataFrame(data = data, schema = ["name", "designation", "city"])
df.printSchema()
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### b) Create PySpark DataFrame by reading files

# COMMAND ----------

# replace the file_path with the source file location which you have downloaded.

df_2 = spark.read.format("csv").option("header", True).load(file_path)
df_2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Note: Here, I will be using the manually created dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Dropping entire duplicate records

# COMMAND ----------

df.dropDuplicates().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Drop duplicate records based on selected columns

# COMMAND ----------

df.dropDuplicates(subset=["name", "designation"]).show()
