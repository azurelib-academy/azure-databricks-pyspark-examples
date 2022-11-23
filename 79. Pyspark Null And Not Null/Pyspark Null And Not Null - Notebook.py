# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark Null And Not Null in Azure Databricks

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
    (1,"Philis",None,None),
    (2,"Issiah",None,"American"),
    (3,"Moishe","9134728713",None),
    (4,"Vivianna","8272404634",None),
    (5,"Bendix",None,None)
]

df = spark.createDataFrame(data, schema=["id","name","phone","address"])
df.printSchema()
df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### b) Create PySpark DataFrame by reading files

# COMMAND ----------

file_path = "/mnt/practice/contacts.csv"
# replace the file_path with the source file location which you have downloaded.

df_2 = spark.read.format("csv").option("header", True).load(file_path)
df_2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Note: Here, I will be using the manually created dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1) isnull()

# COMMAND ----------

from pyspark.sql.functions import col, isnull

# Method 1:
df.select("id", "name", col("phone").isNull().alias("has_no_phone")).show()

# Method 2:
df.select("id", "name", isnull("phone").alias("has_no_phone")).show()

# Note: Since the above two functions return bool value, it can be used with filter() or where() to fetch the non-empty records
df.filter(isnull("phone")) \
.select("id","name","phone").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2) isNotnull()

# COMMAND ----------

from pyspark.sql.functions import col

df.select("id", "name", col("phone").isNotNull().alias("has_phone")).show()

# Note: Since the above two functions return bool value, it can be used with filter() or where() to fetch the non-empty records
df.filter(col("phone").isNotNull()) \
.select("id","name","phone").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3) Using multiple conditions

# COMMAND ----------

from pyspark.sql.functions import col

df.filter(
    (col("phone").isNull()) & (col("address").isNotNull())
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4) SQL way of using

# COMMAND ----------

df.createOrReplaceTempView("contacts")

# Example 1: isNull
spark.sql('''
    SELECT id, name, phone FROM contacts
    WHERE phone IS NULL
''').show()

# Example 2: isNotNull
spark.sql('''
    SELECT id, name, phone FROM contacts
    WHERE phone IS NOT NULL
''').show()

# Example 3: Multiple expression
spark.sql('''
    SELECT * FROM contacts
    WHERE phone IS NULL and address IS NULL
''').show()