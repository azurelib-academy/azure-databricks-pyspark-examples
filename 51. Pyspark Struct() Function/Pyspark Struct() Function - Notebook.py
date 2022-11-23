# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark Struct() Function in Azure Databricks

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
# MAGIC ##### 1. Create an instance of an ArrayType

# COMMAND ----------

data = [
    ("Mamie","Treharne"),
    ("Erv","Colam"),
    ("Daren","Salliss"),
    ("Vania","Laundon"),
    ("Jay","Kees"),
]

df = spark.createDataFrame(data, schema=["f_name","l_name"])
df.printSchema()
df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### b) Create PySpark DataFrame by reading files

# COMMAND ----------

file_path = "/mnt/practice/customer_name.csv"
# replace the file_path with the source file location which you have downloaded.

df_2 = spark.read.format("csv").option("header", True).load(file_path)
df_2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Note:
# MAGIC - The create_map() expects a positive even number of arguments
# MAGIC - Each odd column acts as Key and even column acts as value

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Creating a a struct column

# COMMAND ----------

from pyspark.sql.functions import struct, col

# Method 1:
df_3 = df.select(struct("f_name", "l_name").alias("name"))

# Method 2:
df_3 = df.select(struct(["f_name", "l_name"]).alias("name"))

# Method 3:
df_3 = df.select(struct([col("f_name"), col("l_name")]).alias("name"))

# Method 4:
columns = ("f_name", "l_name")
df_3 = df.select(struct(*columns).alias("name"))

df_3.printSchema()
df_3.show()

# COMMAND ----------

# 2. Using withColumn()

from pyspark.sql.functions import struct

df_4 = df.withColumn("name", struct("f_name", "l_name"))
df_4.show()