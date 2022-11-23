# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark Broadcast Variable in Azure Databricks

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
    (1,"Nils","M",74),
    (2,"Alena","F",93),
    (3,"Ammamaria","F",60),
    (4,"Yardley","M",46),
    (5,"Peadar","M",60),
    (6,"Horace","M",80)
]

df = spark.createDataFrame(data, schema=["id","name","gender","marks"])
df.printSchema()
df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### b) Create PySpark DataFrame by reading files

# COMMAND ----------

file_path = "/mnt/practice/student.csv"
# replace the file_path with the source file location which you have downloaded.

df_2 = spark.read.format("csv").option("header", True).load(file_path)
df_2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Note: Here, I will be using the manually created dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Creating broadcast variable

# COMMAND ----------

gender_map = {"M": "Male", "F": "Female"}
broadcast_gender = sc.broadcast(gender_map)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Accessing a broadcast variable

# COMMAND ----------

broadcast_gender.value["M"]
broadcast_gender.value["F"]

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Working with RDD

# COMMAND ----------

rdd = df.rdd
rdd = rdd.map(lambda t: (t[0],t[1],broadcast_gender.value[t[2]],t[3]))
rdd.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4. Working with DataFrame

# COMMAND ----------

from pyspark.sql.functions import col, udf

# 1. User defined function
def convert_gender(char):
    return broadcast_gender.value[char]

# 2. Registering UDF
convert_gender = udf(convert_gender)

# 3. Use it accordingly
df_2 = df.withColumn("gender", convert_gender(col("gender")))
df_2.show()