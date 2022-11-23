# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark Dataframe To Rdd in Azure Databricks

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
    ("Mamie","Treharne"),
    ("Erv","Colam"),
    ("Daren","Salliss"),
    ("Vania","Laundon"),
    ("Jay","Kees")
]

df = spark.createDataFrame(data, schema=["f_name","l_name"])
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### b) Create PySpark DataFrame by reading files

# COMMAND ----------

# replace the file_paths with the source file location which you have downloaded.

df_2 = spark.read.format("csv").option("header", True).load(file_path)
df_2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Note: Here, I will be using the manually created dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1) Convert DataFrame to RDD

# COMMAND ----------

rdd = df.rdd
print(type(rdd)) # <- Checks the type of rdd variable
rdd.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2) RDD function

# COMMAND ----------

rdd1 = rdd.map(lambda tup: (tup[0], len(tup[0])))
rdd1.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3) Converting RDD to DataFrame

# COMMAND ----------

rdd1.toDF(["f_name", "len_name"]).show()