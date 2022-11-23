# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark Read Csv Files in Azure Databricks

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
 |-- lap_times_1.csv
 |-- lap_times_2.csv
 |-- read_directory
     |-- lap_3.csv
     |-- lap_times_1.csv
     |-- lap_times_2.csv
"""

# COMMAND ----------

# replace the file_path with the source file location of yours.
base_path = "/mnt/practice/read_write_csv"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Read single CSV file by varioud methods

# COMMAND ----------

# Varioud methods of reading file

# Method 1:
df = spark.read.format("csv").load(f"{base_path}/lap_times_1.csv")
# Method 2:
df = spark.read.load(format="csv", path=f"{base_path}/lap_times_1.csv")
# Method 3:
df = spark.read.format("csv").option("path", f"{base_path}/lap_times_1.csv").load()
# Method 4:
df = spark.read.csv(f"{base_path}/lap_times_1.csv")

df.printSchema()
df.show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Read multiple CSV file

# COMMAND ----------

df = spark.read.option("header", True).csv(f"{base_path}/lap_times_1.csv")
print(f"First file count: {df.count()}")

df = spark.read.option("header", True).csv(f"{base_path}/lap_times_2.csv")
print(f"Second file count: {df.count()}")

# Reading multiple files appends the second file to the first.
multiple_files = [f"{base_path}/lap_times_1.csv", f"{base_path}/lap_times_2.csv"]
df = spark.read.option("header", True).csv(multiple_files)
print(f"Mulitple file count: {df.count()}")

# As you know, we have two files each of which has 50 records, 2 * 50 = 100 records excluding headers.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Read multiple file using wildcard

# COMMAND ----------

df = spark.read.option("header", True).csv(f"{base_path}/lap_times_*.csv")
print(f"Multiple file count using wildcard(*): {df.count()}")

# As you know, we have two files each of which has 50 records, 2 * 50 = 100 records excluding headers.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4. Read files from directory

# COMMAND ----------

df = spark.read.option("header", True).csv(f"{base_path}/read_directory/")
print(f"Directory file count: {df.count()}")

# As you know, we have two files each of which has 50 records, 3 * 50 = 150 records excluding headers.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 5. Commonly used CSV read option

# COMMAND ----------

# 1. header
spark.read.option("header", False).csv(f"{base_path}/lap_times_1.csv").show(3)
spark.read.option("header", True).csv(f"{base_path}/lap_times_1.csv").show(3)

# COMMAND ----------

# 2. delimiter
spark.read.option("delimiter", "|").csv(f"{base_path}/lap_times_1.csv").show(3, truncate=False)
spark.read.option("delimiter", ",").csv(f"{base_path}/lap_times_1.csv").show(3)

# COMMAND ----------

# 3. inferSchema
spark.read.option("header", True) \
.option("inferSchema", False).csv(f"{base_path}/lap_times_1.csv").printSchema()

spark.read.option("header", True) \
.option("inferSchema", True).csv(f"{base_path}/lap_times_1.csv").printSchema()

# COMMAND ----------

# 4. nullValue -> make a specifing values to null
spark.read.option("nullValue", "841").csv(f"{base_path}/lap_times_1.csv").show(3)

# COMMAND ----------

# 5. timestampFormat -> Parse the string time format to time format, but it needs a defined schema

print("a) Without schema:")
spark.read \
.option("header", True) \
.option("timestampFormat", "m:ss.SSS") \
.csv(f"{base_path}/lap_times_1.csv") \
.select("time").printSchema()

# --------------------------------------------

from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, TimestampType

lap_schema = StructType([
    StructField('raceId', IntegerType()),
    StructField('driverId', IntegerType()),
    StructField('lap', IntegerType()),
    StructField('position', DoubleType()),
    StructField('time', TimestampType()), # Change me later -> 01:38.0
    StructField('milliseconds', IntegerType())
])

print("b) With schema:")
df = spark.read \
.schema(lap_schema) \
.option("header", True) \
.option("timestampFormat", "m:ss.SSS") \
.csv(f"{base_path}/lap_times_1.csv") \
.select("time").printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 5. Setting multiple option using options

# COMMAND ----------

df = spark.read \
.options(header=True, inferSchema=True, nullValues="841") \
.csv(f"{base_path}/lap_times_1.csv")

df.printSchema()
df.show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 6. Writing CSV files

# COMMAND ----------

# 1. options

df.write.options(header=True).save("target_location")

# COMMAND ----------

# 2. Saving mode:
# a) overwrite – mode is used to overwrite the existing file.
# b) append – To add the data to the existing file.
# c) ignore – Ignores write operation when the file already exists.
# d) error(default) – When the file already exists, it returns an error.

df.write.mode("overwrite").save("target_location")