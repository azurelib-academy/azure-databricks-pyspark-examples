# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark From_Json() Function in Azure Databricks

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
# MAGIC ##### 1. Convert column of json string values into maptype in simple way

# COMMAND ----------

from pyspark.sql.types import MapType, StringType
from pyspark.sql.functions import from_json

# Method 1:
df1 = df.withColumn("value", from_json("value", MapType(StringType(),StringType())).alias("map_col"))

# Method 2:
df1 = df.select(from_json(df.value, "MAP<STRING,STRING>").alias("map_col"))

df1.printSchema()
df1.select("map_col.Name", "map_col.Origin", "map_col.Year").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Convert column of json string values into maptype using DDL format schema

# COMMAND ----------

from pyspark.sql.functions import from_json

ddl_schema = "Name String, Miles_per_Gallon INT, Cylinders INT, Displacement INT, Horsepower INT, Weight_in_lbs INT, Acceleration INT, Year Date, Oring String"

df2 = df.select(from_json("value", ddl_schema).alias("map_col"))
df2.printSchema()
df2.select("map_col.Name", "map_col.Cylinders", "map_col.Horsepower").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Convert column of json string values into maptype using struct()

# COMMAND ----------

from pyspark.sql.types import MapType, StringType, IntegerType, DateType
from pyspark.sql.functions import from_json

schema = StructType([
    StructField("Name", StringType()),
    StructField("Miles_per_Gallon", IntegerType()),
    StructField("Cylinders", IntegerType()),
    StructField("Displacement", IntegerType()),
    StructField("Horsepower", IntegerType()),
    StructField("Weight_in_lbs", IntegerType()),
    StructField("Acceleration", IntegerType()),
    StructField("Year", DateType()),
    StructField("Origin", StringType()),
])

df3 = df.select(from_json("value", schema).alias("map_col"))
df3.printSchema()
df3.select("map_col.Name", "map_col.Acceleration", "map_col.Year").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4. Convert multi line json string values into maptype

# COMMAND ----------

from pyspark.sql.types import MapType, StringType, IntegerType, DateType
from pyspark.sql.functions import from_json, explode

data = [(1, "[{'Name':'chevrolet chevelle malibu', 'Miles_per_Gallon':18, 'Cylinders':8},{'Name':'buick skylark 320', 'Miles_per_Gallon':15, 'Cylinders':8},{'Name':'plymouth satellite', 'Miles_per_Gallon':18, 'Cylinders':8}]")]

schema = ArrayType(
    StructType([
        StructField("Name", StringType()),
        StructField("Miles_per_Gallon", IntegerType()),
        StructField("Cylinders", IntegerType()),
    ]))

df4 = spark.createDataFrame(data, ("key", "value"))
df4 = df4.select(from_json("value", schema).alias("map_col"))
df4.select("map_col.Name").show(truncate=False)