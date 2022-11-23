# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark Read Parquet Files in Azure Databricks

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
 |-- stocks_1.json
 |-- stocks_2.json
 |-- read_directory
     |-- stocks_info_1.json
     |-- stocks_info_2.json
     |-- stocks_3.json
"""

# COMMAND ----------

# replace the file_path with the source file location of yours.
base_path = "/mnt/practice/read_write_parquet"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Read single CSV file by varioud methods

# COMMAND ----------

# Varioud methods of reading file

# Method 1:
df = spark.read.format("parquet").load(f"{base_path}/stocks_1.parquet")
# Method 2:
df = spark.read.load(format="parquet", path=f"{base_path}/stocks_1.parquet")
# Method 3:
df = spark.read.format("parquet").option("path", f"{base_path}/stocks_1.parquet").load()
# Method 4:
df = spark.read.parquet(f"{base_path}/stocks_1.parquet")

df.printSchema()
df.show(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Read multiple JSON file

# COMMAND ----------

df = spark.read.parquet(f"{base_path}/stocks_1.parquet")
print(f"First file count: {df.count()}")

df = spark.read.parquet(f"{base_path}/stocks_2.parquet")
print(f"Second file count: {df.count()}")

# Reading multiple files appends the second file to the first.
multiple_files = [f"{base_path}/stocks_1.parquet", f"{base_path}/stocks_2.parquet"]
df = spark.read.parquet(*multiple_files)
print(f"Mulitple file count: {df.count()}")

# As you know, we have two files each of which has 10 records, 2 * 10 = 20 records.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Read multiple file using wildcard

# COMMAND ----------

df = spark.read.parquet(f"{base_path}/read_directory/stocks_info_*.parquet")
print(f"Multiple file count using wildcard(*): {df.count()}")

# As you know, we have two files each of which has 10 records, 2 * 10 = 40 records.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4. Read files from directory

# COMMAND ----------

df = spark.read.parquet(f"{base_path}/read_directory/")
print(f"Directory file count: {df.count()}")

# As you know, we have two files each of which has 50 records, 3 * 10 = 30 records excluding headers.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 5. Writing Parquet files

# COMMAND ----------

# Saving mode:
# a) overwrite – mode is used to overwrite the existing file.
# b) append – To add the data to the existing file.
# c) ignore – Ignores write operation when the file already exists.
# d) error(default) – When the file already exists, it returns an error.

df.write.mode("overwrite").save("target_location")