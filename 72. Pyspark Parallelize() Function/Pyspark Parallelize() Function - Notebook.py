# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark Parallelize() Function in Azure Databricks

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
# MAGIC ##### 1. Creating an RDD

# COMMAND ----------

# Creating an RDD
rdd_1 = sc.parallelize([1,2,3,4,5,6,7,8,9,10])
print(f"Number of partitions: {rdd_1.getNumPartitions()}")
print(f"Data: {rdd_1.collect()}")

# COMMAND ----------

# Setting up number of partition manually
rdd_2 = sc.parallelize([1,2,3,4,5,6,7,8,9,10], numSlices=2)
print(f"Number of partitions: {rdd_2.getNumPartitions()}")
rdd_2.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2) Creating an empty RDD

# COMMAND ----------

# Method 1:
rdd_3 = sc.emptyRDD()
rdd_3.collect()

# Method 2:
rdd_4 = sc.parallelize([])
rdd_4.collect()

# COMMAND ----------

# Checing whether an RDD is empty or not

# Non-empt RDD
print(f"First RDD is empty: {rdd_1.isEmpty()}")
print(f"Second RDD is empty: {rdd_2.isEmpty()}")

# Empty RDD
print(f"Third RDD is empty: {rdd_3.isEmpty()}")
print(f"Forth RDD is empty: {rdd_4.isEmpty()}")