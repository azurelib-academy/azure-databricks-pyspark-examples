# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark Rdd To Dataframe in Azure Databricks

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
# MAGIC ##### Create RDD

# COMMAND ----------

columns = ["name", "dept", "salary"]
employees_data = [
    ("Kumar", "Sales", 25000),
    ("Shankar", "IT", 32000),
    ("Kavitha", "HR", 27000)
]

rdd = sc.parallelize(employees_data)
rdd.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Convert RDD to DataFrame

# COMMAND ----------

# 1. Using toDF() function

# a) without column name
df1 = rdd.toDF()
df1.printSchema()
df1.show()

# b) Using toDF() with column name
df2 = rdd.toDF(schema=columns)
df2.printSchema()
df2.show()

# COMMAND ----------

# 2. Using createDataFrame() function

# a) without column name
df3 = spark.createDataFrame(rdd)
df3.printSchema()
df3.show()

# b) with column name
df4 = spark.createDataFrame(rdd, schema=columns)
df4.printSchema()
df4.show()