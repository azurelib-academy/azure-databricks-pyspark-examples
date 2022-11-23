# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark Sql Json Functions in Azure Databricks

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
# MAGIC ##### Commonly used JSON functions

# COMMAND ----------

from pyspark.sql.types import StringType, MapType
from pyspark.sql.functions import from_json

# 1. from_json() : Converts JSON string into Struct type or Map type
map_df = df.withColumn("value", from_json("value", MapType(StringType(),StringType())))
map_df.printSchema()
map_df.show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import to_json

# 2. to _json() : Converts MapType or Struct type to JSON string.
json_df = map_df.withColumn("value", to_json("value"))
json_df.printSchema()
json_df.show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import json_tuple

# 3. json_tuple() : Extract the Data from JSON and create them as a new columns.
ind_df = json_df\
.select("id", json_tuple(json_df.value, "Name", "Miles_per_Gallon", "Cylinders", "Displacement", "Horsepower", "Weight_in_lbs", "Acceleration", "Year", "Origin")) \
.toDF("id", "Name", "Miles_per_Gallon", "Cylinders", "Displacement", "Horsepower", "Weight_in_lbs", "Acceleration", "Year", "Origin")
ind_df.printSchema()
ind_df.show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import get_json_object

# 4. get_json_object() : Extracts JSON element from a JSON string based on json path specified.
df.select("id", \
          get_json_object("value", "$.Name").alias("Name"), \
          get_json_object("value", "$.Origin").alias("Origin"), \
         ).show()

# COMMAND ----------

from pyspark.sql.functions import schema_of_json, lit

# 5. schema_of_json() : Create schema string from JSON string
json_string = df.select("value").collect()[0]["value"]

schema_df = schemaStr=spark.range(1) \
    .select(schema_of_json(lit(json_string)))

print(schema_df.collect()[0][0])