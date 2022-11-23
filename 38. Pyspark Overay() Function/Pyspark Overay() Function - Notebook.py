# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark Overay() Function in Azure Databricks

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
    (1,"Mrs.","12_Gal","M","Male"),
    (2,"Dr.","12_Nichol","F","Female"),
    (3,"Rev.","12_Ileane","F","Female"),
    (4,"Rev.","12_Jeffry","M","Male"),
    (5,"Ms.","12_Aryn","F","Female")
]

df = spark.createDataFrame(data, schema=["id","title","name","gender_1","gender_2"])
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### b) Create PySpark DataFrame by reading files

# COMMAND ----------

file_path = "/mnt/practice/people.csv"
# replace the file_path with the source file location which you have downloaded.

df_2 = spark.read.format("csv").option("header", True).load(file_path)
df_2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Note: Here, I will be using the manually created dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Replacing one column value with another

# COMMAND ----------

from pyspark.sql.functions import overlay

df.withColumn("gender_1", overlay("gender_1", "gender_2", 1)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Replacing specific portion of a column value with another

# COMMAND ----------

from pyspark.sql.functions import overlay

# Example 1: No length
df \
.withColumn("title_name", overlay("name", "title", 1)) \
.select("id", "title", "name", "title_name").show()

# Example 2: Length = 0
df \
.withColumn("title_name", overlay("name", "title", 1, 0)) \
.select("id", "title", "name", "title_name").show()

# Example 3: Length = 3
df \
.withColumn("title_name", overlay("name", "title", 1, 3)) \
.select("id", "title", "name", "title_name").show()

# COMMAND ----------

