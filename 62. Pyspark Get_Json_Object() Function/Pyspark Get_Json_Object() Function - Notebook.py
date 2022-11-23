# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark Get_Json_Object() Function in Azure Databricks

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
# MAGIC ##### 1. Extracting single column out of JSON string column

# COMMAND ----------

from pyspark.sql.functions import get_json_object, col

# Method 1:
df.select(get_json_object("value", "$.Name").alias("car_name")) \
.show(truncate=False)

# Method 2:
df.select(get_json_object(col("value"), "$.Origin").alias("car_origin")) \
.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Extracting multiple columns out of JSON string column

# COMMAND ----------

from pyspark.sql.functions import get_json_object, col

df.select(
    get_json_object("value", "$.Name").alias("car_name"),
    get_json_object(col("value"), "$.Origin").alias("car_origin"),
).show(truncate=False)

# COMMAND ----------

