# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark Sort Records in Azure Databricks

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
    (1, "Ramman", 25, "20000", "Chennai"),
    (2, "Suman", 23, "18000", "Coimbatore"),
    (3, "Keerthi", 25, "50000", "Bangalore"),
    (4, "Victor", 32, None, "Mumbai"),
    (5, "Adithya", 40, "50000", "Noida")
]

df = spark.createDataFrame(data = data, schema = ["id","name","age","salary","city"])
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
# MAGIC ##### 1. Ordering data ascendingly

# COMMAND ----------

from pyspark.sql.functions import col, asc

# Method 1
df.orderBy("salary", ascending = True).show()

# Method 2
df.orderBy(asc("salary")).show()

# Method 3
df.orderBy(col("salary").asc()).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Ordering data descendingly

# COMMAND ----------

from pyspark.sql.functions import col, desc

# Method 1
df.orderBy("salary", ascending = False).show()

# Method 2
df.orderBy( desc("salary") ).show()

# Method 3
df.orderBy( col("salary").desc() ).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Order data based on multiple columns

# COMMAND ----------

from pyspark.sql.functions import col

# 1. Order by Ascending based on multiple columns
df.orderBy("salary", "city").show()

# 2. Order by Ascending or Descending based on multiple columns
df.orderBy(col("salary").asc(), col("city").desc()).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4. Order by considering null values

# COMMAND ----------

from pyspark.sql.functions import col

# 1. Order column by Ascending, but null values at top
df.orderBy(col("salary").asc_nulls_first()).show()

# 2. Order column by Ascending, but null values at last
df.orderBy(col("salary").asc_nulls_last()).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 5. Selecting columns in reverse order

# COMMAND ----------

df.select(df.columns[::-1]).show()