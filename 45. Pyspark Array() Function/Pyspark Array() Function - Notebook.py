# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark Array() Function in Azure Databricks

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
# MAGIC ##### 1. Create an instance of an ArrayType

# COMMAND ----------

data = [
    ("Anandhi", "Chennai", "Cochin", "Hyderabad"),
    ("Benish", "Coimbatore", "Mumbai", "Chennai"),
    ("Chandru", "Salem", "Bangalore", None),
    ("Derif", "Delhi", "Bangalore", "Noida"),
    ("Fayaz", "Mumbai", "Pune", "Cochin"),
    ("Gomathi", "Chennai", "Mumbai", None),
    ("Harini", "Delhi", "Noida", "Kolkata")
]

columns = ["name", "pref1", "pref2", "pref3"]
df = spark.createDataFrame(data, schema=columns)
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### b) Create PySpark DataFrame by reading files

# COMMAND ----------

file_path = "/mnt/practice/preference.json"
# replace the file_path with the source file location which you have downloaded.

df_2 = spark.read.format("json").load(file_path)
df_2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Join two column into an array

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3.1. Using select()

# COMMAND ----------

from pyspark.sql.functions import array, col

# Method 1:
df.select("name", array("pref1", "pref2", "pref3").alias("preferences")).show(truncate=False)

# Method 2:
prefs_col = ["pref1", "pref2", "pref3"]
df.select("name", array(*prefs_col).alias("preferences")).show(truncate=False)

# Method 3:
prefs_col = ["pref1", "pref2", "pref3"]
df \
.select("name", array([col(pref) for pref in prefs_col]).alias("preferences"))\
.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3.2. Using withColumn()

# COMMAND ----------

from pyspark.sql.functions import array, col

# Method 1:
df.withColumn("preferences", array("pref1", "pref2", "pref3")) \
.select("name", "preferences").show(truncate=False)

# Method 2:
prefs_col = ["pref1", "pref2", "pref3"]
df.withColumn("preferences", array(*prefs_col)) \
.select("name", "preferences").show(truncate=False)

# Method 3:
prefs_col = ["pref1", "pref2", "pref3"]
df \
.withColumn("preferences", array([col(pref) for pref in prefs_col]))\
.select("name", "preferences").show(truncate=False)