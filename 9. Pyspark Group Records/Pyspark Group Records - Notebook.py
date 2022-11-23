# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark Group Records in Azure Databricks

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
    (1, "Anandhi", "IT", 35000, "KL"),
    (2, "Manju", "IT", 50000, "KL"),
    (3, "Kannan", "HR", 34000, "DL"),
    (4, "Bharathi", "HR", 43000, "DL"),
    (5, "Kutty", "Sales", 10000, "KL"),
    (6, "Balaji", "Sales", 15000, "TN"),
    (7, "Elango", "Sales", 25000, "TN"),
]

df = spark.createDataFrame(data, schema=["id","name","department","salary","state"])
df.printSchema()
df.show(truncate=False)

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
# MAGIC ##### 1. Single aggregation

# COMMAND ----------

df.groupBy("department").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Multiple aggregations

# COMMAND ----------

from pyspark.sql.functions import max, min

df.groupBy("department").agg(
    max("salary"),
    min("salary")
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Multiple aggregations using multiple columns

# COMMAND ----------

from pyspark.sql.functions import max, avg

df.groupBy("department", "state").agg(
    avg("salary"),
    max("salary")
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4. Renaming aggregated columns

# COMMAND ----------

from pyspark.sql.functions import max, avg

df.groupBy("department").agg(
    avg("salary").alias("avg_salary"),
    max("salary").alias("max_salary")
).show()