# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Pyspark Rdd in Azure Databricks

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
# MAGIC ##### 1) Creating RDD using paralelize()

# COMMAND ----------

rdd1 = sc.parallelize([1,2,3,4,5])
print(type(rdd1))
rdd1.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2) Creating RDD by reading files

# COMMAND ----------

# replace the file_path with the source file location which you have downloaded.

rdd2 = sc.textFile(file_path)
print(type(rdd2))
rdd2.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3) Creating empty RDD

# COMMAND ----------

# Method 1:
rdd3 = sc.emptyRDD()
rdd3.collect()

# Method 2:
rdd4 = sc.parallelize([])
rdd4.collect()