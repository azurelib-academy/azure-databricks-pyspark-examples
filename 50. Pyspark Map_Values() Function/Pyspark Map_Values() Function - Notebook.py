# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark Map_Values() Function in Azure Databricks

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

data = [
    ("Arasan", ({"age": "23", "blood": "B+"})),
    ("Karthik", ({"age": "24", "blood": "B+"})),
    ("Sanjay", ({"age": "24", "blood": None, "state": "TN"})),
    ("Marish", None)
]

df = spark.createDataFrame(data, schema=["name", "info"])
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### b) Create PySpark DataFrame by reading files

# COMMAND ----------

file_path = "/mnt/practice/people_age.csv"
# replace the file_path with the source file location which you have downloaded.

df_2 = spark.read.format("json").load(file_path)
df_2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Note:
# MAGIC - The create_map() expects a positive even number of arguments
# MAGIC - Each odd column acts as Key and even column acts as value

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Get the values of MapType column

# COMMAND ----------

from pyspark.sql.functions import map_values

df.select(map_values("info")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4. Get all the unique values used in the MapType column

# COMMAND ----------

from pyspark.sql.functions import map_values, explode

# 1. Unique keys
unique_values_df = df \
.select(explode(map_values("info")).alias("values")).distinct() \
.filter("values IS NOT NULL")
unique_values_df.show()

# 2. Collecting all the numeric value out of all values
unique_values_list = [record.values for record in unique_values_df.rdd.collect() if record.values.isnumeric()]
print(unique_values_list)