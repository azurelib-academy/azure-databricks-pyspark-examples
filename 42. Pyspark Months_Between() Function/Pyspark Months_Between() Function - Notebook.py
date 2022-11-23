# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark Months_Between() Function in Azure Databricks

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
    ("2019-01-11","2021-04-12","2019-09-17 12:02:21","2021-07-12 18:29:29"),
    ("2019-08-04","2021-04-15","2018-11-11 14:17:05","2021-08-03 16:21:40"),
    ("2019-03-24","2021-02-08","2019-02-07 04:26:49","2020-11-28 05:20:33"),
    ("2019-04-13","2021-06-05","2019-07-08 20:04:09","2021-05-18 08:21:12"),
    ("2019-02-22","2021-10-01","2018-11-28 05:46:54","2021-06-17 21:39:42")
]

columns = ["from_date","to_date","from_datetime","to_datetime"]
df = spark.createDataFrame(data, schema=columns)
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### b) Create PySpark DataFrame by reading files

# COMMAND ----------

file_path = "/mnt/practice/date_diff.csv"
# replace the file_path with the source file location which you have downloaded.

df_2 = spark.read.format("csv").option("header", True).load(file_path)
df_2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Note: Here, I will be using the manually created dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Number of months between two dates using DataFrame

# COMMAND ----------

from pyspark.sql.functions import months_between, floor

# 1. using select()
df.select("from_date",
          floor(months_between("to_date", "from_date")).alias("months_between"),
          "to_date").show()

# 2. using withColumn()
df.withColumn("months_between", floor(months_between("to_datetime", "from_datetime"))) \
.select("to_datetime", "months_between", "from_datetime").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Number of months between two dates using SQL expression

# COMMAND ----------

df.createOrReplaceTempView("days")

spark.sql("""
SELECT
    from_date,
    floor(months_between(to_date, from_date)) AS months_between,
    to_date
FROM days
""").show()