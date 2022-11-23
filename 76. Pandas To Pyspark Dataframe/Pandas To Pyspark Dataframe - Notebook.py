# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pandas To Pyspark Dataframe in Azure Databricks

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
# MAGIC ##### Create a Pandas DataFrame

# COMMAND ----------

import pandas as pd

data = [
    [1, "Anand"],
    [2, "Bernald"],
    [3, "Chandran"],
    [4, "Delisha"],
    [5, "Maran"],
]

pandas_df = pd.DataFrame(data, columns = ["id", "name"])
print(pandas_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Convert Pandas to PySpark DataFrame

# COMMAND ----------

pyspark_df = spark.createDataFrame(pandas_df)
pyspark_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Change coumn name and data type while converting Pandas to PySpark DataFrame

# COMMAND ----------

# Method 1: Using StructType

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

new_schema = StructType([
    StructField("emp_id", IntegerType()),
    StructField("emp_name", StringType()),
])

pyspark_df = spark.createDataFrame(pandas_df, schema=new_schema)
pyspark_df.printSchema()
pyspark_df.show()

# COMMAND ----------

# Method 2: Using DDL Format

ddl_schema = "empId STRING, empName STRING"

pyspark_df = spark.createDataFrame(pandas_df, schema=ddl_schema)
pyspark_df.printSchema()
pyspark_df.show()