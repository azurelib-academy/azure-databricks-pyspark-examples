# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark Split Column Values in Azure Databricks

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
# MAGIC ##### 1. Split() on DataFrame

# COMMAND ----------

from pyspark.sql.functions import split

s_df1 = df.select(split("names_1", ",").alias("comma_delimiter"))
s_df1.printSchema()
s_df1.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Split() using SQL expression

# COMMAND ----------

df.createOrReplaceTempView("names")

spark.sql("SELECT SPLIT(names_1, ',') AS comma_delimiter FROM names") \
.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Split() using limit parameter

# COMMAND ----------

from pyspark.sql.functions import split, size

# 1. Limit = 2
# 2. The resulting array's last entry will contain all the input beyond the last match pattern
# 3. The array length = limit

df.select(
    split("names_2", "\|", limit=2).alias("limit_delimiter"),
    size(split("names_2", "\|", limit=2)).alias("length"),
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4. Spliting the column with you have multiple delimiter

# COMMAND ----------

from pyspark.sql.functions import split, size

df.select(
    split("names_3", "[, |]").alias("multiple_delimiter"),
    size(split("names_3", "[, |]")).alias("length"),
).show(truncate=False)