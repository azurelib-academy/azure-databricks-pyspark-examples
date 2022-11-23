# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark Translate() Function in Azure Databricks

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
    ("Andie,Ebbutt","Andie|Ebbutt|Ebbutt","Andie,Ebbutt Andie|Ebbutt"),
    ("Hobie,Deegan","Hobie|Deegan|Deegan","Hobie,Deegan Hobie|Deegan"),
    ("Denys,Belverstone","Denys|Belverstone|Belverstone","Denys,Belverstone Denys|Belverstone"),
    ("Delphine,Pietersma","Delphine|Pietersma|Pietersma","Delphine,Pietersma Delphine|Pietersma"),
    ("Putnem,Chasson","Putnem|Chasson|Chasson","Putnem,Chasson Putnem|Chasson")
]

df = spark.createDataFrame(data, schema=["names_1","names_2","names_3"])
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### b) Create PySpark DataFrame by reading files

# COMMAND ----------

file_path = "/mnt/practice/name_list.csv"
# replace the file_path with the source file location which you have downloaded.

df_2 = spark.read.format("csv").option("header", True).load(file_path)
df_2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Note: Here, I will be using the manually created dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Replace one character

# COMMAND ----------

from pyspark.sql.functions import translate

# 1. translate with select
df.select("names_1", translate("names_1", ",", "*").alias("names_translate")).show()

# 2. translate with withColumn
df.withColumn("names_translate", translate("names_1", ",", "+"))\
.select("names_1", "names_translate").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Replace multiple characters by character

# COMMAND ----------

from pyspark.sql.functions import translate

# 1. translate with select
df.select("names_3", 
          translate("names_3", ", |", "123").alias("names_translate")
         ).show(truncate=False)

# 2. translate with withColumn
df.withColumn("names_translate", translate("names_3", ", |", "123"))\
.select("names_3", "names_translate")\
.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Remove selected characters

# COMMAND ----------

from pyspark.sql.functions import translate

# Example 1:
df.select("names_3", 
          translate("names_3", ", |", "").alias("names_translate")
         ).show(truncate=False)

# Example 2:
df.withColumn("names_translate", translate("names_3", ", |", "*"))\
.select("names_3", "names_translate")\
.show(truncate=False)