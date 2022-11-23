# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark Explode() Function in Azure Databricks

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
    ("Anand",["Apple", "Banana"],[["Simrun", "Kuspu"], ["Ajith", "Suriya"]]),
    ("Berne",["Pine Apple", "Orange"],[["Trisha", "Samantha"], ["Vijay", "Suriya"]]),
    ("Charan",["Grapes", "Pome"],[["Nayanthara", "Jothika"], ["Simbu", "Vishal"]]),
    ("Denish",["Starwberry", "Blueberry"],[["Asin", "Katrina"], ["Rajini", "Aswin"]]),
    ("Eren",None,[["Sherina", "Emi"], ["Kamal", "Sivaji"]])
]

columns = ["name","foods","actors"]
df = spark.createDataFrame(data, schema=columns)
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### b) Create PySpark DataFrame by reading files

# COMMAND ----------

file_path = "/mnt/practice/date_diff.csv"
# replace the file_path with the source file location which you have downloaded.

df_2 = spark.read.format("csv").option("header", True).load(file_path)
df_2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Note: Here, I will be using the manually created dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. explode()

# COMMAND ----------

from pyspark.sql.functions import explode

df.select("name", explode("foods").alias("explode")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. explode_outer()

# COMMAND ----------

from pyspark.sql.functions import explode_outer

df.select("name", explode_outer("foods").alias("explode_outer")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. posexplode()

# COMMAND ----------

from pyspark.sql.functions import posexplode

df.select("name", posexplode("foods").alias("index", "posexplode")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4. posexplode_outer()

# COMMAND ----------

from pyspark.sql.functions import posexplode_outer

df.select("name", posexplode_outer("foods").alias("index", "posexplode_outer")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 5. flatten()

# COMMAND ----------

from pyspark.sql.functions import flatten

df.select("actors", flatten("actors")).show(truncate=False)