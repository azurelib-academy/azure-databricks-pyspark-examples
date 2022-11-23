# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark Schema_Of_Json() Function in Azure Databricks

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
# MAGIC ##### 1. Generate column DDL format schema from JSON column

# COMMAND ----------

# Step 1: Get the value of the first JSON column

first_json_record = df.select("value").collect()[0]["value"]
print(first_json_record)

# COMMAND ----------

# Step 2: Pass the extracted value to the schema_of_json() function
options = {
    'allowUnquotedFieldNames':'true',
    'allowSingleQuotes':'true',
    'allowNumericLeadingZeros': 'true'}

json_schema = df.select(
    schema_of_json(lit(first_json_record), options).alias("json_struct")
).collect()[0][0]

print(str("Extract schema in DDL format:\n") + json_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Un-wrapping all the sub-columns using derived DDL format schema

# COMMAND ----------

# 1. Convert Json column to Struct column

from pyspark.sql.functions import from_json

json_df = df.select("id", from_json("value", json_schema).alias("json"))
json_df.printSchema()

# COMMAND ----------

# 2. Get the sub-column list
sub_columns = json_df.select("json.*").columns
sub_columns = [str("json.") + column for column in sub_columns]
print("Modified sun columns:\n" + str(sub_columns))

# COMMAND ----------

# 3. Pass and unwrap the sub_columns inside the select()
json_df.select("id", *sub_columns).show()