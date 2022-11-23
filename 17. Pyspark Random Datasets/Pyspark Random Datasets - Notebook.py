# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark Random Datasets in Azure Databricks

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
# MAGIC ##### 1. Create manual PySpark DataFrame

# COMMAND ----------

df=spark.range(100)
df.printSchema()
df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Create PySpark DataFrame by reading files

# COMMAND ----------

# replace the file_path with the source file location which you have downloaded.

df_2 = spark.read.format("csv").option("inferSchema", True).option("header", True).load(file_path)
df_2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Note: Here, I will be using the manually created dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Producing a subset of sample records using sample() function in PySpark Dataframe

# COMMAND ----------

# First run
print(f"First run count: {df.sample(fraction=0.1).count()}")
sample_df.show()

# Second run
print(f"Second run count: {df.sample(fraction=0.1).count()}")
sample_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Reproducing same sample records using sample() function in PySpark Dataframe

# COMMAND ----------

sample_df = df.sample(fraction=0.1, seed=324)
# Note: seed value, ensure the giving the same sample data on each run

# First run
print(f"First run count: {sample_df.count()}")
sample_df.show()

# Second run
print(f"Second run count: {sample_df.count()}")
sample_df.show()

# Third run with another seed value -> different sampling record
sample_df_2 = df.sample(fraction=0.1, seed=456)
print(f"Third run count with another seed value: {sample_df_2.count()}")
sample_df_2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Reproducing sample records with duplicate value a sample() function in PySpark

# COMMAND ----------

sample_df_3 = df.sample(withReplacement=True, fraction=0.2, seed=124)
print(f"First duplicate run output: {', '.join([str(each.id) for each in sample_df_3.collect()])}")

sample_df_4 = df.sample(withReplacement=True, fraction=0.2, seed=133)
print(f"Second duplicate run output: {', '.join([str(each.id) for each in sample_df_4.collect()])}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4. Producing sample records based on column using sampleBy() function in PySpark

# COMMAND ----------

df2 = df.withColumn("key", df.id % 3).select("key", "id")
print("Grouping the dataframe based on 'key' column:")
df2.groupBy("key").count().show()

# 10% of 34 records = 3.4
df3 = df2.sampleBy("key", fractions={0: 0.1, 1: 1}, seed=123)
print("Number of sample records reproduced based on fractions:")
df3.groupBy("key").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 5. Producing sample records of RDD using sampleBy() & takeSample() function in PySpark

# COMMAND ----------

rdd = spark.sparkContext.range(0,100)

# 1. sample()
rdd.sample(withReplacement=False, fraction=0.1,seed=1).collect()

# COMMAND ----------

# 2. takeSample() -> action -> send result to driver
rdd.takeSample(withReplacement=False, num=10,seed=1)