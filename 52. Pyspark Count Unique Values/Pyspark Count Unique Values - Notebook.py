# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark Count Unique Values in Azure Databricks

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
    (1,"1000","Pontiac"),
    (2,"Ram Van B350",None),
    (3,"SRX","Cadillac"),
    (4,None,None),
    (5,"CLK-Class",None),
    (6,"GLC",None),
    (7,"Jetta",None),
    (8,None,None),
    (9,"3500 Club Coupe",None),
    (10,"Tacoma",None)
]

columns = ["id","model","make"]
df = spark.createDataFrame(data, schema=columns)
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### b) Create PySpark DataFrame by reading files

# COMMAND ----------

file_path = "/mnt/formulaoneadls/practice/cars_detail.csv"
# replace the file_path with the source file location which you have downloaded.

df_2 = spark.read.format("csv").option("inferSchema", True).option("header", True).load(file_path)
df_2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Note: Here, I will be using the manually created dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Count single column distinct values

# COMMAND ----------

from pyspark.sql.functions import countDistinct

df.select(countDistinct("model")).show()

# Note: countDistinct avoids null

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Count multiple columns distinct value

# COMMAND ----------

from pyspark.sql.functions import countDistinct

df.select(countDistinct("model", "make")).show()

# Note: Always returns smaller distinct value

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Using distinct().count()

# COMMAND ----------

df.select("make").distinct().show()
df.select("make").distinct().count()

# Note: distinct() considers null