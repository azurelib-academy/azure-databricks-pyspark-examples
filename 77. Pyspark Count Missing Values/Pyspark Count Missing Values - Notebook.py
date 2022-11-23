# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark Count Missing Values in Azure Databricks

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
# MAGIC ##### Create manual PySpark DataFrame

# COMMAND ----------

import numpy as np

data = [
    (1, "", None),
    (2, "null", None),
    (3, "NULL", np.NaN),
    (4, "Suresh", np.NaN),
    (5, "James", np.NaN)
]

df = spark.createDataFrame(data, schema=["id","name","salary"])
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Count empty string

# COMMAND ----------

from pyspark.sql.functions import col, when, length, count

# method 1:
df.select(
    count(
        when(col("name") == "", 1)
    ).alias("1. Empty string")
 ).show()

# Method 2:
df.select(
    count(
        when(length(col("name")) == 0, 1)
    ).alias("2. Empty string")
 ).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Count null string

# COMMAND ----------

from pyspark.sql.functions import col, when, lower

df.select(
    count(
        when(lower(col("name")) == "null", 1) # case-sensitive
    ).alias("Null string")
 ).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Count None value

# COMMAND ----------

from pyspark.sql.functions import col, when, isnull

# Method 1:
df.select(
    count(
        when(isnull(col("salary")), 1)
    ).alias("1. None value")
 ).show()

# Method 2:
df.select(
    count(
        when(col("salary").isNull(), 1)
    ).alias("2. None value")
 ).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4. Count Numpy NaN

# COMMAND ----------

from pyspark.sql.functions import col, when, isnan

df.select(
    count(
        when(isnan(col("salary")), 1)
    ).alias("NaN value")
 ).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 5. Check all type of null values

# COMMAND ----------

from pyspark.sql.functions import col, when, isnull, isnan, count, lower

df.select([count(when(
    isnan(col(each_col)) | \
    (col(each_col) == "") | \
    isnull(col(each_col)) | \
    (lower(col(each_col)) == "null"), 1 \
)).alias(f"{each_col} -> count") for each_col in df.columns]).show()