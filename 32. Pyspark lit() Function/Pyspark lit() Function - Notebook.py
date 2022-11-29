# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create New Column With Constant Values in Azure Databricks

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
# MAGIC ##### a) Create manual PySpark DataFrame

# COMMAND ----------

data = [
    ("Mamie","Treharne"),
    ("Erv","Colam"),
    ("Daren","Salliss"),
    ("Vania","Laundon"),
    ("Jay","Kees"),
]

df = spark.createDataFrame(data, schema=["f_name","l_name"])
df.printSchema()
df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### b) Create PySpark DataFrame by reading files

# COMMAND ----------

file_path = "/mnt/practice/customer_name.csv"
# replace the file_path with the source file location which you have downloaded.

df_2 = spark.read.format("csv").option("header", True).load(file_path)
df_2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Note: Here, I will be using the manually created dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Add new columns of different datatypes using lit() with select

# COMMAND ----------

from pyspark.sql.functions import lit, array

# 1. Integer value
int_df = df.select("*", lit(100).alias("int_col"))
int_df.printSchema()
int_df.show()

# 2. String value
str_df = df.select("*", lit("a_string").alias("str_col"))
str_df.printSchema()
str_df.show()

# 3. Array value
# Note: As typelit() is not available in PySpark now, I have created an ArrayType column using array()
arr_df = df.select("*", array([lit(1), lit(2)]).alias("array_col"))
arr_df.printSchema()
arr_df.show()

# 4. Map value
# Note: As typelit() is not available in PySpark now, I have created an MapType column using from_json()
from pyspark.sql.types import StringType, MapType
from pyspark.sql.functions import from_json

map_str_df = df.select("*", lit("{'key': 'value'}").alias("map_str_col"))
map_df = map_str_df.withColumn("map_col", 
                               from_json("map_str_col", MapType(StringType(), StringType())))
map_df.printSchema()
map_df.show()

# from_json() -> converts JSON string to desired DataType

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Add new columns of different datatypes using lit() with withColumn

# COMMAND ----------

from pyspark.sql.functions import lit, array

# 1. Integer value
int_df = df.withColumn("int_col",lit(100))
int_df.printSchema()
int_df.show()

# 2. String value
str_df = df.withColumn("str_col", lit("a_value"))
str_df.printSchema()
str_df.show()

# 3. Array value
arr_df = df.withColumn("array_col", array([lit(1), lit(2)]))
arr_df.printSchema()
arr_df.show()

# 4. Map value
from pyspark.sql.types import StringType, MapType
from pyspark.sql.functions import from_json

map_str_df = df.withColumn("map_str_col", lit("{'key': 'value'}"))
map_df = map_str_df.withColumn("map_col", 
                               from_json("map_str_col", MapType(StringType(), StringType())))
map_df.printSchema()
map_df.show()

# from_json() -> converts JSON string to desired DataType

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Add a literal value while concating two colmns

# COMMAND ----------

from pyspark.sql.functions import col, lit, concat

df.withColumn("full_name", concat("f_name", lit(" "), "l_name")).show()

# Note: In PySpark string value inside a column is considered a column name.