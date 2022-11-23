# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark Percent_Rank() Function in Azure Databricks

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
    ("Lewis","McLaren",10.0),
    ("Nick","McLaren",2.0),
    ("Nico","McLaren",6.0),
    ("Fernando","McLaren",3.0),
    ("Heikki","McLaren",8.0),
    ("Kazuki","Ferrari",9.0),
    ("Sébastien","Ferrari",7.0),
    ("Kimi","Ferrari",6.0)
]

df = spark.createDataFrame(data, schema=["driver_name","team","points"])
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### b) Create PySpark DataFrame by reading files

# COMMAND ----------

# replace the file_path with the source file location which you have downloaded.

df_2 = spark.read.format("csv").option("inferSchema", True).option("header", True).load(file_path)
df_2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Note: Here, I will be using the manually created dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Relativity rank of each records based on points

# COMMAND ----------

from pyspark.sql.functions import percent_rank
from pyspark.sql.window import Window

window_spec = Window.orderBy("points")
# The window orderBy() -> acts as on which order the row has be numbered

df \
.withColumn("percent_rank", percent_rank().over(window_spec)) \
.select("driver_name", "team", "points", "percent_rank").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Relativity rank of each records based on points within a window

# COMMAND ----------

from pyspark.sql.functions import percent_rank
from pyspark.sql.window import Window

window_spec = Window.partitionBy("team").orderBy("points")
# The window partitionBy() -> acts as groupBy

df \
.withColumn("percent_rank", percent_rank().over(window_spec)) \
.select("driver_name", "team", "points", "percent_rank").show()