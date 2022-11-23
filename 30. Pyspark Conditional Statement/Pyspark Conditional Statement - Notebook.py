# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark Conditional Statement in Azure Databricks

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
    (27,190,43,11.9),
    (28,187,28,8.0),
    (29,163,72,27.1),
    (30,179,68,21.2),
    (31,153,54,23.1),
    (32,178,23,7.3),
    (33,195,29,7.6),
    (34,160,59,23.0),
    (35,157,69,28.0),
    (36,189,59,16.5)
]

df = spark.createDataFrame(data, schema=["id","height","weight","bmi"])
df.printSchema()
df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### b) Create PySpark DataFrame by reading files

# COMMAND ----------

file_path = "/mnt/formulaoneadls/practice/bmi.csv"
# replace the file_path with the source file location which you have downloaded.

df_2 = spark.read.format("csv").option("header", True).load(file_path)
df_2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Note: Here, I will be using the manually created dataframe

# COMMAND ----------

# Hint
# 1. Under weight : BMI < 18.5
# 2. Normal       : BMI >= 18.5 and BMI < 25
# 3. Over weight  : BMI >= 25

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. When().otherwise()

# COMMAND ----------

from pyspark.sql.functions import when, col

# 1. Select()
df.select("*", when(col("bmi") < 18.5, "under-weight").alias("status")).show(5)

# 2. withColumn()
df \
.withColumn("status", when(col("bmi") < 18.5, "under-weight") \
.otherwise("unknown").alias("status")) \
.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. CASE WHEN

# COMMAND ----------

from pyspark.sql.functions import expr

# 1. Select()
df.select("*", expr("""
                    CASE
                    WHEN bmi >= 25 THEN 'over-weight'
                    ELSE 'unknown'
                    END as Status
                    """)
         ).show(5)

# 2. withColumn()
df.withColumn("status", expr("""
                            CASE
                            WHEN bmi >= 25 THEN 'over-weight'
                            ELSE 'unknown'
                            END AS status
                            """)
             ).show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Multiple condition

# COMMAND ----------

from pyspark.sql.functions import when, col, expr

# 1. Using withColumn
df.select("*", 
           when(col("bmi") < 18.5, "under-weight") \
          .when(col("bmi") >= 25, "over-weight") \
          .otherwise("normal").alias("status")          
         ).show(5)

# 2. Using CASE WHEN
df.select("*", expr("""
                    CASE
                    WHEN bmi < 18.5 THEN 'under-weight'
                    WHEN bmi >= 25 THEN 'over-weight'
                    ELSE 'normal'
                    END AS status
                    """)         
         ).show(5)