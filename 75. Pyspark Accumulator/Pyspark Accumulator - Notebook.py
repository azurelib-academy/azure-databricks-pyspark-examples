# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark Accumulator in Azure Databricks

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
# MAGIC ##### a) Create manual PySpark RDD

# COMMAND ----------

data = [
    (1,"Nils","M",74),
    (2,"Alena","F",93),
    (3,"Ammamaria","F",60),
    (4,"Yardley","M",46),
    (5,"Peadar","M",60)
]

rdd = sc.parallelize(data)
rdd.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### b) Create PySpark RDD from DataFrame by reading files

# COMMAND ----------

file_path = "/mnt/practice/student.csv"
# replace the file_path with the source file location which you have downloaded.

df = spark.read.format("csv").option("header", True).load(file_path)
rdd = df.rdd
rdd.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Note: Here, I will be using the manually created RDD

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Creating accumulator variable

# COMMAND ----------

# Accumulator are initialized only once

# Interger
int_acc = sc.accumulator(0)

# Float
float_acc = sc.accumulator(0.0)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Accessing a accumulator variable

# COMMAND ----------

print(f"Integer value: {int_acc.value}")
print(f"Float value: {float_acc.value}")

# Note: Accumulator values are called by Driver

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Working with RDD

# COMMAND ----------

# Example: Sum operation

total_marks = sc.accumulator(0)
rdd.foreach(lambda t: total_marks.add(t[3]))
print(f"The class total marks: {total_marks.value} out of {rdd.count() * 100}.")

# COMMAND ----------

# Example: Count operation

student_count = sc.accumulator(0)
rdd.foreach(lambda _: student_count.add(1))
print(f"Total number of sudents: {student_count.value}")