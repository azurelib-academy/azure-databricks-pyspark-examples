# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark Collect Function in Azure Databricks

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
    ("SUB001","Tamil"),
    ("SUB002","English"),
    ("SUB003","Maths"),
    ("SUB004","Science"),
    ("SUB005","Social")
]

df = spark.createDataFrame(data, schema=["subject_id","subject_name"])
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
# MAGIC ##### 1. Collect records of RDD and DataFrame

# COMMAND ----------

# 1. RDD
rdd = df.rdd
rdd.collect()

# 2. DataFrame
df.rdd.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Making use fo collected records

# COMMAND ----------

for record in df.collect():
    print(f"The subject id of {record.subject_name} is {record.subject_id.upper()}.")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Collect specific records

# COMMAND ----------

# 1. First two rows
df.collect()[:2]

# COMMAND ----------

# 2. First row, first column value
df.collect()[0][0]