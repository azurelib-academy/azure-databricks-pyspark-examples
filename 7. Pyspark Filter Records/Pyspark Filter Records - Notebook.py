# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark Filter Records in Azure Databricks

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
    ("Sentra",25,8,"USA"),
    ("Titan",13,10,"Japan"),
    ("1000",25,8,"Japan"),
    ("Voyager",19,8,"USA"),
    ("Cabriolet",14,10,"Japan")
]

df = spark.createDataFrame(data, schema=["name","miles","cylinders","origin"])
df.printSchema()
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### b) Create PySpark DataFrame by reading files

# COMMAND ----------

# replace the file_path with the source file location which you have downloaded.

df_2 = spark.read.format("csv").option("header", True).load(file_path)
df_2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Note: Here, I will be using the manually created dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Filter records based on condition

# COMMAND ----------

from pyspark.sql.functions import col

# Method 1
df.filter("cylinders = 8").show()

# Method 3
df.filter(df.cylinders == 8).show()

# Method 3
df.filter(col("cylinders") == 8).show()

# Method 4
df.filter(df["cylinders"] == 8).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Filter records based on multiple condition

# COMMAND ----------

from pyspark.sql.functions import col

# Method 1
df.filter("cylinders = 8 AND origin != 'USA'").show()

# Method 2
df.filter( (df.cylinders == 8) & (df.origin != "USA") ).show()

# Method 3
df.filter( (col("cylinders") == 8) & (col("origin") != "USA") ).show()

# Method 4
df.filter( (df["cylinders"] == 8) & ~(df["origin"] == "USA") ).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Filter records based on array values

# COMMAND ----------

from pyspark.sql.functions import col

origin_list = ["Japan", "German"]
df.filter(col("origin").isin(origin_list)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4. Filter records using string function

# COMMAND ----------

# Filter based on Starts With, Ends With, Contains

# 1. Starts with car name as 'T'
df.filter(col('name').startswith('T')).show()

# 2. Ends with origin as 'an'
df.filter(col('origin').endswith('an')).show()

# 3. Contains 'ya' in car name
df.filter(col('name').contains('ya')).show()