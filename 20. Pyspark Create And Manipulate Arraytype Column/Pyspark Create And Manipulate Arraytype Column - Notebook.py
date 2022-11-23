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
# MAGIC #### Pyspark Create And Manipulate Arraytype Column in Azure Databricks

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
# MAGIC ##### 1. Create an instance of an ArrayType

# COMMAND ----------

from pyspark.sql.types import ArrayType, IntegerType

array_column = ArrayType(elementType=IntegerType(), containsNull=True)
# Array of Integer values, which can have null values

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Create an ArrayType column using StructType()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, ArrayType, StringType

schema = StructType([
    StructField("full_name", StringType(), True),
    StructField("items", ArrayType(StringType()), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
])

data = [
    ("Indhu Madhi", (["cakes", "cookies", "chocolates"]), "Salem", "TN"),
    ("Asha Shree", (["apple", "orange", "banana"]), "Cochin", "KL"),
    ("Anand Kumar", (["carrot", "banana", "cookies"]), "Erode", "TN"),
    ("Aswin Raj", None, "Whitefield", "KA")
]

# If you want to read data from a file, I have attached the dataset in the blog.

df = spark.createDataFrame(data, schema=schema)
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Array Functions

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3.1. explode() & posexplode()

# COMMAND ----------

from pyspark.sql.functions import explode, posexplode, explode_outer, posexplode_outer

# 1. explode()
df.select("full_name", explode("items").alias("foods")).show()

# 2. explode_outer()
df.select("full_name", explode_outer("items").alias("foods")).show()

# 3. posexplode()
df.select("full_name", posexplode("items").alias("food_index", "foods")).show()

# 4. posexplode_outer()
df.select("full_name", posexplode_outer("items").alias("food_index", "foods")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3.2. split()

# COMMAND ----------

df.select(split("full_name", " ").alias("split_name")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3.3. Join two column into an array

# COMMAND ----------

from pyspark.sql.functions import array

df.select("full_name", array("city", "state").alias("address")).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3.4. Array contains

# COMMAND ----------

from pyspark.sql.functions import array_contains

df.select("full_name", "items").filter(array_contains("items", "cookies")).show(truncate=False)