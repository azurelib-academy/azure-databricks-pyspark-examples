# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark Count() Function in Azure Databricks

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
    (1,"Em","VN"),
    (2,"Emalia","DE"),
    (3,"Odette",None),
    (4,"Mandy","DE"),
    (4,"Mandy","DE")
]

df = spark.createDataFrame(data, schema=["id","name","country"])
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### b) Create PySpark DataFrame by reading files

# COMMAND ----------

# replace the file_paths with the source file location which you have downloaded.

df_2 = spark.read.format("csv").option("header", True).load(file_path)
df_2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Note: Here, I will be using the manually created dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1) count() using DataFrame

# COMMAND ----------

rows = df.count()
print(f"Number of rows: {rows}")

uniques_rows = df.distinct().count()
print(f"Number of unique rows: {uniques_rows}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2) count() function

# COMMAND ----------

from pyspark.sql.functions import col, count

df.select(count("id"), count("name"), count("country")).show()

# Note: count() function ignores null/None value

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3) GroupBy count()

# COMMAND ----------

from pyspark.sql.functions import count

# Method 1:
df.select("country").groupBy("country").count().show()

# Method 2:
df.groupBy("country").agg(count("country").alias("count")).show()

# Method 3:
df.groupBy("country").agg({'country': 'count'}) \
.withColumnRenamed("count(country)", "count").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4) Count using SQL expression

# COMMAND ----------

df.createOrReplaceTempView("people")

# Example 1: Count
spark.sql('SELECT COUNT(*) FROM people').show()

# Example 2: Distinct count
spark.sql('SELECT COUNT(DISTINCT country) FROM people').show()

# Example 3: Groupby count
spark.sql('''
    SELECT country, COUNT(country)
    FROM people
    GROUP BY country
''').show()