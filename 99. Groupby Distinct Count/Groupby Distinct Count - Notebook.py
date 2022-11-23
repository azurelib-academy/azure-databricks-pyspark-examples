# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Groupby Distinct Count in Azure Databricks

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
    (1,"Reuben","Delhi","Infosys"),
    (2,"Aldrich","Kerala","CTS"),
    (3,"Ede","Tamil Nadu","CTS"),
    (4,"Benjamin","Tamil Nadu","Infosys"),
    (5,"Adler","Mumbai","Infosys"),
    (6,"Jolynn","Mumbai","CTS"),
    (7,"Daile","Kerala","TCS"),
    (8,"Cullin","Mumbai","TCS"),
    (9,"Yul","Tamil Nadu","TCS"),
    (10,"Valaree","Mumbai","Infosys")
]

df = spark.createDataFrame(data, schema=["id","name","state","company"])
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
# MAGIC ##### 1) Using distinct count function

# COMMAND ----------

from pyspark.sql.functions import count_distinct, countDistinct

# Method 1:
df.groupBy("company").agg(count_distinct("state").alias("distinct_count")).show()

# Method 2:
df.groupBy("company").agg(countDistinct("state").alias("distinct_count")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2) Using groupby and count functions

# COMMAND ----------

df \
.groupBy("company", "state").count() \
.groupBy("company").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3) GroupBy Count Distinct using SQL expression

# COMMAND ----------

df.createOrReplaceTempView("employees")

spark.sql('''
    SELECT company, COUNT(DISTINCT state) AS distinct_conut
    FROM employees
    GROUP BY company
''').show()