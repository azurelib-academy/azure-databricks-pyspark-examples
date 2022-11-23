# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark Empty Rdd Or Dataframe in Azure Databricks

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
# MAGIC ##### 1. Creating empty RDD

# COMMAND ----------

# Method 1:
sc.emptyRDD().collect()

# Method 2:
sc.parallelize([]).collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Creating empty DataFrame

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# a) Without schema
spark.createDataFrame(sc.emptyRDD(), schema=StructType([])).show()

# b) With schema
with_schema = StructType([
    StructField("first_name", StringType()),
    StructField("last_name", StringType()),
    StructField("age", IntegerType())])

spark.createDataFrame(sc.emptyRDD(), schema=with_schema).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Converting empty RDD to DataFrame

# COMMAND ----------

ddl_schema = "first_name STRING, last_name STRING, age INT"
sc.emptyRDD().toDF(schema=ddl_schema).printSchema()