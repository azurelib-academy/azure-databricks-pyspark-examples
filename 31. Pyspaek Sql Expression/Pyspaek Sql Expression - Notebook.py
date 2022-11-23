# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspaek Sql Expression in Azure Databricks

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
    ("Andie","Ebbutt","Polygender","2022-03-22","$30.10"),
    ("Hobie","Deegan","Male","2021-12-29","$285.99"),
    ("Denys","Belverstone","Female","2022-01-07","$212.91"),
    ("Delphine","Pietersma","Female","2022-09-17","$724.81"),
    ("Putnem","Chasson","Male","2021-12-03","$938.47"),
    ("Jenilee","Hindmoor","Polygender","2022-10-10","$243.23"),
    ("Nicoline","Cowitz","Female","2022-08-28","$32.53"),
    ("Brunhilde","Vasyukhnov","Female","2022-04-10","$237.17"),
    ("Roxi","Leming","Female","2022-02-15","$304.06"),
    ("Raffaello","Cornes","Male","2022-06-30","$631.73")
]

df = spark.createDataFrame(data, schema=["first_name","last_name","gender","created","balance"])
df.printSchema()
df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### b) Create PySpark DataFrame by reading files

# COMMAND ----------

file_path = "/mnt/practice/customer.csv"
# replace the file_path with the source file location which you have downloaded.

df_2 = spark.read.format("csv").option("header", True).load(file_path)
df_2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Note: Here, I will be using the manually created dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC ##### SQL functions

# COMMAND ----------

from pyspark.sql.functions import expr

# 1. Concate columns
df.select("first_name", "last_name", expr("CONCAT(first_name, last_name)")).show(3)

# COMMAND ----------

from pyspark.sql.functions import expr

# 2. Column alias
df.select(expr("first_name AS fore_name")).show(3)

# COMMAND ----------

from pyspark.sql.functions import expr

# 3. Substring
df.select(expr("SUBSTRING(balance, 2) AS balance")).show(3)

# COMMAND ----------

from pyspark.sql.functions import expr

# 4. Cast
print("a) Before casting:")
df.select(expr("SUBSTRING(balance, 2) AS balance")).printSchema()

print("b) After casting:")
df.select(expr("CAST(SUBSTRING(balance, 2) AS INT) AS balance")).printSchema()

# COMMAND ----------

from pyspark.sql.functions import expr

# 5. Arimtic operation
df.select("balance", expr("SUBSTRING(balance, 2) * 100 AS multiple_balance")).show(3)

# COMMAND ----------

from pyspark.sql.functions import expr

# 6. CASE WHEN
df.select( "gender", 
           expr("""
                CASE
                WHEN gender = 'Male' THEN 'M'
                ELSE '-'
                END AS new_gender
                """)).show(3)