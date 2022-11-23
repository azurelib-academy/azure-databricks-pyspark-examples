# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark Read Json Files in Azure Databricks

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

"""
File structure:
/mnt/practice/read_write_csv/
 |-- drivers_1.json
 |-- drivers_2.json
 |-- multi_line.json
 |-- single_quote.json
 |-- read_directory
     |-- drivers_1.json
     |-- drivers_1.json
     |-- drivers_info_3.json
"""

# COMMAND ----------

# replace the file_path with the source file location of yours.
base_path = "/mnt/practice/read_write_json"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Read single CSV file by varioud methods

# COMMAND ----------

# Varioud methods of reading file

# Method 1:
df = spark.read.format("json").load(f"{base_path}/drivers_1.json")
# Method 2:
df = spark.read.load(format="json", path=f"{base_path}/drivers_1.json")
# Method 3:
df = spark.read.format("json").option("path", f"{base_path}/drivers_1.json").load()
# Method 4:
df = spark.read.json(f"{base_path}/drivers_1.json")

df.printSchema()
df.show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Read multiple JSON file

# COMMAND ----------

df = spark.read.json(f"{base_path}/drivers_1.json")
print(f"First file count: {df.count()}")

df = spark.read.json(f"{base_path}/drivers_2.json")
print(f"Second file count: {df.count()}")

# Reading multiple files appends the second file to the first.
multiple_files = [f"{base_path}/drivers_1.json", f"{base_path}/drivers_2.json"]
df = spark.read.json(multiple_files)
print(f"Mulitple file count: {df.count()}")

# As you know, we have two files each of which has 20 records, 2 * 20 = 40 records.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Read multiple file using wildcard

# COMMAND ----------

df = spark.read.json(f"{base_path}/read_directory/drivers_*.json")
print(f"Multiple file count using wildcard(*): {df.count()}")

# As you know, we have two files each of which has 20 records, 2 * 20 = 40 records.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4. Read files from directory

# COMMAND ----------

df = spark.read.json(f"{base_path}/read_directory/")
print(f"Directory file count: {df.count()}")

# As you know, we have two files each of which has 50 records, 3 * 20 = 60 records excluding headers.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 5. Commonly used CSV read option

# COMMAND ----------

# 1. dateFormat -> Parse the string date format to time format, but it needs a defined schema

print("a) Without schema:")
spark.read \
.option("dateFormat", "yyyy-MM-dd") \
.json(f"{base_path}/drivers_1.json") \
.select("dob").printSchema()

# --------------------------------------------

from pyspark.sql.types import StructType, StructField, IntegerType, DateType, StringType

drivers_schema =  StructType([
    StructField('driverId', IntegerType()),
    StructField('driverRef', StringType()),
    StructField('number', IntegerType()),
    StructField('code', StringType()),
    StructField('name', StructType([StructField('forename', StringType()), StructField('surname', StringType())])),
    StructField('dob', DateType()), # yyyy-MM-dd
    StructField('nationality', StringType()),
    StructField('url', StringType())
])

print("b) With schema:")
spark.read \
.schema(drivers_schema) \
.option("dateFormat", "yyyy-MM-dd") \
.json(f"{base_path}/drivers_1.json") \
.select("dob").printSchema()

# COMMAND ----------

# 2. allowSingleQuotes -> treats single quotes, the way you treat double quotes in JSON

print("a) Before allowing")
spark.read \
.option("allowSingleQuotes", False) \
.json(f"{base_path}/single_quote.json") \
.printSchema()

print("b) Before allowing")
spark.read \
.option("allowSingleQuotes", True) \
.json(f"{base_path}/single_quote.json") \
.printSchema()

# COMMAND ----------

# 3. Multi line JSON

spark.read \
.option("multiLine", True) \
.json(f"{base_path}/multi_line.json") \
.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 5. Setting multiple option using options

# COMMAND ----------

df = spark.read \
.schema(drivers_schema) \
.options(dateFormat="yyyy-MM-dd", allowSingleQuotes=True) \
.json(f"{base_path}/drivers_1.json")

df.printSchema()
df.show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 6. Writing JSON files

# COMMAND ----------

# 1. options

df.write.options(allowSingleQuotes=True).save("target_location")

# COMMAND ----------

# 2. Saving mode:
# a) overwrite – mode is used to overwrite the existing file.
# b) append – To add the data to the existing file.
# c) ignore – Ignores write operation when the file already exists.
# d) error(default) – When the file already exists, it returns an error.

df.write.mode("overwrite").save("target_location")