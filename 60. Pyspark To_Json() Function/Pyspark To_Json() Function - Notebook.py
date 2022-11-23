# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark To_Json() Function in Azure Databricks

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
    {"id": 1, "name": {"first_name": "Etta", "last_name": "Burrel"}, "details": [{"gender": "Female"}, {"age": "46"}], "preferences": ["District of Columbia", "Colorado"]},
    {"id": 2, "name": {"first_name": "Ky", "last_name": "Fiddyment"}, "details": [{"gender": "Male"}, {"age": "35"}], "preferences": ["California", "Massachusetts"]},
    {"id": 3, "name": {"first_name": "Rod", "last_name": "Meineken"}, "details": [{"gender": "Male"}, {"age": "50"}], "preferences": ["North Carolina", "Minnesota"]},
    {"id": 4, "name": {"first_name": "Selestina", "last_name": "Ley"}, "details": [{"gender": "Female"}, {"age": "47"}], "preferences": ["Michigan", "Pennsylvania"]},
    {"id": 5, "name": {"first_name": "Alvan", "last_name": "Shee"}, "details": [{"gender": "Male"}, {"age": "34"}], "preferences": ["Montana", "California"]}
]

df = spark.createDataFrame(data).select("id", "name", "details", "preferences")
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### b) Create PySpark DataFrame by reading files

# COMMAND ----------

file_path = "/mnt/formulaoneadls/practice/customer.json"
# replace the file_path with the source file location which you have downloaded.

df_2 = spark.read.format("json").load(file_path)
df_2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Note: Here, I will be using the manually created dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Convert MapType column to StringType

# COMMAND ----------

from pyspark.sql.functions import to_json

# 1. Converted to MapType column to StringType column

df1 = df.select("name", to_json("name").alias("str_name"))
df1.printSchema()
df1.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Convert List of MapType column to StringType

# COMMAND ----------

from pyspark.sql.functions import to_json

# 2. Converted to List of MapType column to StringType column

df2 = df.select("details", to_json("details").alias("str_details"))
df2.printSchema()
df2.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Convert ArrayType to StringType

# COMMAND ----------

from pyspark.sql.functions import to_json

# 3. Converted to ArrayType column to StringType column

df3 = df.select("preferences", to_json("preferences").alias("str_preferences"))
df3.printSchema()
df3.show(truncate=False)