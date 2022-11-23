# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark Json_Tuple() Function in Azure Databricks

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

json_string = "{'Name':'chevrolet', 'Miles_per_Gallon':18, 'Cylinders':8, 'Displacement':307, 'Horsepower':130, 'Weight_in_lbs':3504, 'Acceleration':12, 'Year':'1970-01-01', 'Origin':'USA'}"

df = spark.createDataFrame([(1, json_string)], schema=["id", "value"])
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### b) Create PySpark DataFrame by reading files

# COMMAND ----------

file_path = "/mnt/formulaoneadls/practice/json_data.csv"
# replace the file_path with the source file location which you have downloaded.

df_2 = spark.read.format("csv").option("header", True).load(file_path)
df_2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Note: Here, I will be using the manually created dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Creating new columns out of JSON string column

# COMMAND ----------

from pyspark.sql.functions import json_tuple, col

# Method 1:
df.select(json_tuple("value", "Name").alias("car_name")) \
.show(truncate=False)

# Method 2:
df.select(json_tuple(col("value"), "Name", "Origin").alias("car_name", "car_origin")) \
.show(truncate=False)

# Method 3:
extract_col_names = ["Name", "Displacement", "Horsepower"]
new_col_names = [str("car_") + col_name.lower() for col_name in extract_col_names]

df.select(json_tuple("value", *extract_row_names).alias(*new_col_names)) \
.show(truncate=False)