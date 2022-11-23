# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark Self Join in Azure Databricks

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
    (1,"Hilton",None),
    (2,"Mei",None),
    (3,"Renaldo",2),
    (4,"Marlo",2),
    (5,"Daniela",1)
]

df = spark.createDataFrame(data, schema=["emp_id", "emp_name", "mng_id"])
df.printSchema()
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### b) Create PySpark DataFrame by reading files

# COMMAND ----------

file_path = "/mnt/practice/employee.csv"
# replace the file_path with the source file location which you have downloaded.

df_2 = spark.read.format("csv").option("header", True).load(file_path)
df_2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Note: Here, I will be using the manually created dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1) PySpark Self Join

# COMMAND ----------

from pyspark.sql.functions import col

# Joining df with df

df.alias("a") \
.join(df.alias("b"), col("a.mng_id") == col("b.emp_id"), "left") \
.selectExpr("a.emp_id", "a.emp_name", "b.emp_name as mng_name").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2) PySpark SQL Join

# COMMAND ----------

df.createOrReplaceTempView("employee")

spark.sql('''
    SELECT a.emp_id, a.emp_name, b.emp_name AS mng_name
    FROM employee AS `a`
    LEFT JOIN employee AS `b` ON a.mng_id = b.emp_id
''').show()