# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark Unix Timestamp in Azure Databricks

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
    ("2022-08-08","2022-04-17 17:16:20","19-04-2022 23:02:32"),
    ("2022-04-29","2022-11-07 04:03:11","27-07-2022 18:09:39"),
    ("2022-08-22","2022-02-07 09:15:31","08-11-2022 09:58:34"),
    ("2021-12-28","2022-02-28 02:47:25","03-01-2022 01:59:22"),
    ("2022-02-13","2022-05-22 11:25:29","25-02-2022 04:46:47")
]

df = spark.createDataFrame(data, schema=["date","date_time_1","date_time_2"])
df.printSchema()
df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### b) Create PySpark DataFrame by reading files

# COMMAND ----------

file_path = "/mnt/practice/date_time.csv"
# replace the file_path with the source file location which you have downloaded.

df_2 = spark.read.format("csv").option("inferSchema", True).option("header", True).load(file_path)
df_2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Note: Here, I will be using the manually created dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1) Current time in Unix TImestamp format

# COMMAND ----------

from pyspark.sql.functions import unix_timestamp

df.select(unix_timestamp().alias("curr_unix_time")).show(1)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2) Using unix_timestamp()

# COMMAND ----------

from pyspark.sql.functions import unix_timestamp

df2 = df.select(
    unix_timestamp("date_time_1").alias("proper->unix"),
    unix_timestamp("date_time_2", "dd-MM-yyyy HH:mm:ss").alias("improper->unix"),
    unix_timestamp("date", "yyyy-MM-dd").alias("date_only->unix"))

df2.printSchema()
df2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3) Using from_unixtime()

# COMMAND ----------

from pyspark.sql.functions import from_unixtime

df3 = df2.select(
    from_unixtime("proper->unix", "yyyy-MM-dd").alias("yyyy-MM-dd"),
    from_unixtime("improper->unix", "dd-MM-yyyy").alias("dd-MM-yyyy"),
    from_unixtime("date_only->unix", "dd/MM/yyyy").alias("dd/MM/yyyy")
)

df3.printSchema()
df3.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3) Using SQL expression

# COMMAND ----------

# Example 1:
df.createOrReplaceTempView("date_time")

spark.sql('''
SELECT 
    UNIX_TIMESTAMP(date_time_1) AS `proper->unix`,
    UNIX_TIMESTAMP(date_time_2, "dd-MM-yyyy HH:mm:ss") AS `improper->unix`,
    UNIX_TIMESTAMP(date, "yyyy-MM-dd") AS `date_only->unix`
FROM date_time
''').show()

# COMMAND ----------

# Example 2:
df2.createOrReplaceTempView("unix_time")

spark.sql('''
SELECT 
    FROM_UNIXTIME(`proper->unix`, "yyyy-MM-dd") AS `yyyy-MM-dd`,
    FROM_UNIXTIME(`improper->unix`, "dd-MM-yyyy") AS `dd-MM-yyyy`,
    FROM_UNIXTIME(`date_only->unix`, "dd/MM/yyyy") AS `dd/MM/yyyy`
FROM unix_time
''').show()