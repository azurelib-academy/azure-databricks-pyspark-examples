# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark Collect_List() Function in Azure Databricks

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
    (1,"Humberto","Human Resources",None),
    (2,"Nada","Engineering",500),
    (3,"Letta","Support",700),
    (4,"Garry","Engineering",500),
    (5,"Jeanne","Support",600)
]

columns = ["id","name","dept","salary"]
df = spark.createDataFrame(data, schema=columns)
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### b) Create PySpark DataFrame by reading files

# COMMAND ----------

file_path = "/mnt/practice/employees.csv"
# replace the file_path with the source file location which you have downloaded.

df_2 = spark.read.format("csv").option("header", True).load(file_path)
df_2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Join two column into an array

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3.1. Collect column value with duplication

# COMMAND ----------

from pyspark.sql.functions import collect_list

df.select(collect_list("salary").alias("salaries")).show(truncate=False)

# Note: Look we have duplication here and no null values

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3.2. Using collect_list() as an aggregation function

# COMMAND ----------

# Collect the salaries department wise

from pyspark.sql.functions import collect_list

df.groupBy("dept").agg(collect_list("salary").alias("salaries")).show(truncate=False)

# Note: Look we have duplication here and no null values