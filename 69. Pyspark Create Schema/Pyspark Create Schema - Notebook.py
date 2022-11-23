# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark Create Schema in Azure Databricks

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
# MAGIC ##### 1. Simple structure

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

defined_schema = StructType([
    StructField("first_name", StringType(), nullable=True),
    StructField("last_name", StringType(), nullable=True),
    StructField("age", IntegerType(), nullable=True)
])

data = [("Anand", "Raj", 22),("Benish", "Chris", 42),("Nandhini", "Sree", 52)]
df = spark.createDataFrame(data, schema=defined_schema)
df.printSchema()
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Nested Structure

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

defined_schema = StructType([
    StructField("name", StructType([
        StructField("first_name", StringType(), nullable=True),
        StructField("last_name", StringType(), nullable=True)
    ]), nullable=True),
    StructField("age", IntegerType(), nullable=True)])

data = [(("Anand", "Raj"), 22),(("Benish", "Chris"), 42),(("Nandhini", "Sree"), 52)]
df = spark.createDataFrame(data, schema=defined_schema)
df.printSchema()
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Creating array type column

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType

defined_schema = StructType([
    StructField("array_name", ArrayType(StringType(), True)),
    StructField("age", IntegerType())
])

data = [(("Anand", "Raj"), 22),(("Benish", "Chris"), 42),(("Nandhini", "Sree"), 52)]
df = spark.createDataFrame(data, schema=defined_schema)
df.printSchema()
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4. Creating Map type column

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, MapType, IntegerType

defined_schema = StructType([
    StructField("map_name", MapType(StringType(), StringType())),
    StructField("age", IntegerType())
])

data = [
    (({"first_name":"Anand", "last_name": "Raj"}), 22),
    (({"first_name":"Benish", "last_name": "Chris"}), 42),
    (({"first_name":"Nandhini", "last_name": "Sree"}), 52)]

df = spark.createDataFrame(data, schema=defined_schema)
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 5. Creating a DataFrame using DDL format schema

# COMMAND ----------

ddl_schema = "first_name STRING, last_name STRING, age STRING"

data = [("Anand", "Raj", 22),("Benish", "Chris", 42),("Nandhini", "Sree", 52)]
df = spark.createDataFrame(data, schema=ddl_schema)
df.printSchema()
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 6. Changing DataFrame column structure

# COMMAND ----------

from pyspark.sql.functions import struct, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

defined_schema = StructType([
    StructField("first_name", StringType(), nullable=True),
    StructField("last_name", StringType(), nullable=True),
     StructField("age", IntegerType(), nullable=True)
])

data = [("Anand", "Raj", 22),("Benish", "Chris", 42),("Nandhini", "Sree", 52)]
df = spark.createDataFrame(data, schema=defined_schema)

modified_df = df.withColumn("name", struct(
    col("first_name").alias("f_name"),
    col("last_name").alias("l_name")
)).select("name", "age")

modified_df.printSchema()
modified_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 7. Changing DataFrame structure type

# COMMAND ----------

from pyspark.sql.types import StringType

modified_df = modified_df.withColumn("string_age", col("age").cast(StringType()))
modified_df.printSchema()
modified_df.show()