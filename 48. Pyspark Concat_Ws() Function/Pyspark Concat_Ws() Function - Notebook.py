# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark Concat_Ws() Function in Azure Databricks

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
    (1,"Waldemar",31),
    (2,"Abbey",19),
    (3,"Amalle",30),
    (4,"Uri",23),
    (5,"Dennie",58)
]

columns = ["id","name","age"]
df = spark.createDataFrame(data, schema=columns)
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### b) Create PySpark DataFrame by reading files

# COMMAND ----------

file_path = "/mnt/practice/people_age.csv"
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
# MAGIC ##### 3. Creating a MapType column

# COMMAND ----------

from pyspark.sql.functions import create_map, col

# Method 1:
df.select("*", create_map("name", "age")).show()

# Method 2:
df.select("*", create_map(["name", "age"])).show()

# Method 3:
df.select("*", create_map(col("name"), col("age"))).show()

# Method 4:
df.select("*", create_map([col("name"), col("age")])).show()

# Check schema
df.select("*", create_map([col("name"), col("age")])).printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4. Creating a custom key and value MapType column

# COMMAND ----------

from pyspark.sql.functions import create_map, col, lit, concat

df_3 = df.select("*",  create_map(
    lit("emp_id"), concat(lit("EMP_"), col("id")),
    lit("age"), col("age")
).alias("properties"))

df_3.printSchema()
df_3.show(truncate=False)