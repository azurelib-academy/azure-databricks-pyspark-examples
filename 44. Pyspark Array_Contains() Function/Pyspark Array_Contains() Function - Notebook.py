# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark Array_Contains() Function in Azure Databricks

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

from pyspark.sql.types import StructType, StructField, ArrayType, StringType

data = [
    ("Anand",["Java", "Python"]),
    ("Berne",["Python", "Scala"]),
    ("Charan",["Java Script", "PHP"]),
    ("Denish",["Python", "SQL"]),
    ("Eren",None)
]

columns = ["full_name","languages"]
df = spark.createDataFrame(data, schema=columns)
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### b) Create PySpark DataFrame by reading files

# COMMAND ----------

file_path = "/mnt/practice/json_array.json"
# replace the file_path with the source file location which you have downloaded.

df_2 = spark.read.format("json").load(file_path)
df_2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Check the value inside the array using array_contains()

# COMMAND ----------

from pyspark.sql.functions import array_contains

# 1. Using select()
df.select("*", array_contains("languages", "Python").alias("knowns_python")).show()

# 2. Using withColumn()
df.withColumn("knowns_java", array_contains("languages", "Java")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Filter out column using array_contains()

# COMMAND ----------

df.select("full_name", "languages") \
.filter(array_contains("languages", "Python")) \
.show()

# COMMAND ----------

df.select("*", array_contains("full_name", "a").alias("name")).show()