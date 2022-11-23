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
# MAGIC #### Pyspark Create And Manipulate Maptype Column in Azure Databricks

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
# MAGIC ##### 1. Create an instance of an MapType

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType, MapType

map_column = MapType(IntegerType(),StringType(),False)
# instance of MapType, key: IntegerType() and value: StringType()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Create an MapType column using StructType()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, MapType, StringType

schema = StructType([
    StructField("name", StringType(), True),
    StructField("info", MapType(StringType(), StringType(), True), True)
])

data = [
    ("Arasan", ({"age": "23", "blood": "B+"})),
    ("Karthik", ({"age": "24", "blood": "B-"})),
    ("Sanjay", ({"age": "28", "blood": "O-"})),
    ("Marish", None)
]

# If you want to read data from a file, I have attached the dataset in the blog.

df = spark.createDataFrame(data, schema=schema)
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Access MapType column

# COMMAND ----------

from pyspark.sql.functions import col

# Selecting MapType sub-column in multiple ways

df.select("info.age").show(truncate=False)

df.select(df.info.age).show(truncate=False)
df.select(col("info").age).show(truncate=False)
df.select(col("info")["age"]).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4. Extract key and create new column out of it

# COMMAND ----------

# 1. Using select() function
df.select("name", "info.age", "info.blood").show()

# 2. Using withColumn() function
df.withColumn("age", df.info.age)\
.withColumn("blood", df.info.blood)\
.select("name", "age", "blood")\
.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 5. Functions

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 5.1 explode()

# COMMAND ----------

from pyspark.sql.functions import explode, explode_outer, posexplode, posexplode_outer

# 1. explode
df.select("name", explode("info")).show()

# 2. explode_outer
df.select("name", explode_outer("info")).show()

# 3. posexplode
df.select("name", posexplode("info")).show()

# 4. posexplode_outer
df.select("name", posexplode_outer("info")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 5.2  map_keys()

# COMMAND ----------

from pyspark.sql.functions import map_keys

df.select("name", map_keys("info")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 5.3  map_values()

# COMMAND ----------

from pyspark.sql.functions import map_values

df.select("name", map_values("info")).show()