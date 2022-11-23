# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark Concat_Ws() Function in Azure Databricks

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
    (1, "Andie", "Ebbutt", ['Andie', 'Ebbutt']),
    (2, "Hobie", "Deegan", ['Hobie', 'Deegan']),
    (3, "Denys", "Belverstone", ['Denys', 'Belverstone']),
    (4, "Delphine", "Pietersma", ['Delphine', 'Pietersma']),
    (5, "Putnem", "Chasson", ['Putnem', 'Chasson'])
]

df = spark.createDataFrame(data, schema=["id", "f_name", "l_name", "name_list"])
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### b) Create PySpark DataFrame by reading files

# COMMAND ----------

file_path = "/mnt/practice/name_list.json"
# replace the file_path with the source file location which you have downloaded.

df_2 = spark.read.format("json").load(file_path)
df_2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Note: Here, I will be using the manually created dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Concat ArrayType column into StringType column with separator

# COMMAND ----------

from pyspark.sql.functions import concat_ws

# 1. using select
df.select(concat_ws(",", "name_list").alias("concat_1")).show()

# 2. using withColumn
df.withColumn(
    "concat_2",
    concat_ws("-", "name_list")
).select("concat_2").show()

# 3. using SQL Expression
df.createOrReplaceTempView("name_list")
spark.sql("SELECT CONCAT_WS('||', name_list) AS concat_3 FROM name_list").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Concate multiple columns into single StringType column

# COMMAND ----------

from pyspark.sql.functions import concat_ws

# 1. using select
df.select(concat_ws(",", "id", "f_name").alias("concat_1")).show()

# 2. using withColumn
df.withColumn(
    "concat_2",
    concat_ws("-", "id", "f_name")
).select("concat_2").show()

# 3. using SQL Expression
df.createOrReplaceTempView("name_list")
spark.sql("""SELECT CONCAT_WS('||', id, f_name) AS concat_3 FROM name_list""").show()