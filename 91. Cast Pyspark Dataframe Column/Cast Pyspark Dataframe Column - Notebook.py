# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cast Pyspark Dataframe Column in Azure Databricks

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
    ('1','MurciÃ©lago','Lamborghini','53566.22','2008-05-20','false'),
    ('2','S2000','Honda','47063.49','1984-01-13','false'),
    ('3','LeMans','Pontiac','35654.8','1989-04-27','true'),
    ('4','Suburban 2500','Chevrolet','44013.67','1954-01-06','false'),
    ('5','4Runner','Toyota','56891.95','1959-06-12','true')
]

columns = ["id","model","make","price","release_date","is_petrol"]
df = spark.createDataFrame(data, schema=columns)
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
# MAGIC ##### 1) Casting with select()

# COMMAND ----------

from pyspark.sql.functions import col

df.select(
    col("id").cast("INTEGER"),
    col("price").cast("DOUBLE"),
    col("release_date").cast("DATE"),
    col("is_petrol").cast("BOOLEAN")
).printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2) Casting with withColumn()

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DateType

df \
.withColumn("id", col("id").cast(IntegerType())) \
.withColumn("price", col("price").cast(DoubleType())) \
.withColumn("release_date", col("release_date").cast(DateType())) \
.withColumn("is_petrol", col("is_petrol").cast(BooleanType())) \
.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3) Casting with selectExpr()

# COMMAND ----------

df.selectExpr(
    "CAST(id as INTEGER)",
    "model", "make",
    "CAST(price as DOUBLE)",
    "CAST(release_date as DATE)",
    "CAST(is_petrol as BOOLEAN)"    
).printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4) Casting with SQL expression

# COMMAND ----------

df.createOrReplaceTempView("cars")

spark.sql('''
    SELECT
        INT(id), model, make,
        DOUBLE(price),
        DATE(release_date),
        BOOLEAN(is_petrol)
    FROM cars
''').printSchema()
