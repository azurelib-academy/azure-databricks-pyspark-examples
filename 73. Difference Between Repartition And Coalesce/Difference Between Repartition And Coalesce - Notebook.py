# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Difference Between Repartition And Coalesce in Azure Databricks

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
# MAGIC ##### a) Creating an RDD and see how many partitions we have

# COMMAND ----------

rdd = sc.parallelize(range(1, 9), 4)
rdd.getNumPartitions()

'''
Partitioned data:

Partition 0: [1,2]
Partition 1: [3,4]
Partition 2: [5,6]
Partition 3: [7,8]
'''

# COMMAND ----------

# Increacing RDD partition
rdd = rdd.repartition(8)
rdd.getNumPartitions()

'''
Partitioned data:

Partition 0: [1]
Partition 1: [2]
Partition 2: [3]
Partition 3: [4]
Partition 4: [5]
Partition 5: [6]
Partition 6: [7]
Partition 7: [8]
'''

# COMMAND ----------

# Decreacing RDD partition
rdd = rdd.coalesce(2)
rdd.getNumPartitions()

'''
Partitioned data:

Partition 0: [1,2]
Partition 1: [3,4,5,6,7,8]
'''

# COMMAND ----------

# MAGIC %md
# MAGIC ##### b) Creating a DataFrame and see how many partitions we have

# COMMAND ----------

df = spark.range(16)
df.printSchema()
df.rdd.getNumPartitions()

'''
Partitioned data:

Partition 0: [0,1]
Partition 1: [2,3]
Partition 2: [4,5]
Partition 3: [6,7]
Partition 4: [8,9]
Partition 5: [10,11]
Partition 6: [12,13]
Partition 7: [14,15]
'''

# COMMAND ----------

# Increasing DataFrame partition
df_1 = df.repartition(16)
df_1.rdd.getNumPartitions()

'''
Partitioned data:

Partition 0: [0]
Partition 1: [1]
Partition 2: [2]
Partition 3: [3]
Partition 4: [4]
Partition 5: [5]
Partition 6: [6]
Partition 7: [7]
Partition 8: [8]
Partition 9: [9]
Partition 10: [10]
Partition 11: [11]
Partition 12: [12]
Partition 13: [13]
Partition 14: [14]
Partition 15: [15]
'''

# COMMAND ----------

# Decreasing DataFrame partition
df_2 = df.repartition(4)
df_2.rdd.getNumPartitions()

'''
Partitioned data:

Partition 0: [0,1]
Partition 1: [2,3]
Partition 2: [4,5,6,7,8,9]
Partition 3: [10,11,12,13,14,15]
'''