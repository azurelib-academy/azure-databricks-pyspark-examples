# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark Sql Window Functions in Azure Databricks

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
    ("Lewis","McLaren",10.0),
    ("Nick","McLaren",2.0),
    ("Nico","McLaren",6.0),
    ("Fernando","McLaren",3.0),
    ("Heikki","McLaren",8.0),
    ("Kazuki","Ferrari",9.0),
    ("Sébastien","Ferrari",7.0),
    ("Kimi","Ferrari",6.0)
]

df = spark.createDataFrame(data, schema=["driver_name","team","points"])
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### b) Create PySpark DataFrame by reading files

# COMMAND ----------

# replace the file_path with the source file location which you have downloaded.

df_2 = spark.read.format("csv").option("inferSchema", True).option("header", True).load(file_path)
df_2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Note: Here, I will be using the manually created dataframe

# COMMAND ----------

# Window function
# 		Ranking function -> row_number, rank, dense rank
# 		Analytic function ->  lag, lead
# 		Aggregate functions

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. a) Start by creating window agrregation using orderBy()

# COMMAND ----------

'''
The row_number() window function is used to assign a sequential row number, starting with 1, to each window partition's result.
'''

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.orderBy("driver_name")

df.withColumn("row_number", row_number().over(window_spec)) \
.select("row_number", "driver_name").show()

# Added row number range from 1 to last record in sequence order by 'driver_name' in ascending order.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. b) Ceating window agrregation using partitionBy()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.partitionBy('team').orderBy('driver_name')

df.withColumn('row_number', row_number().over(window_spec)) \
.select("row_number", "team", "driver_name").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Commonly used Ranking function -> rank() and dense_rank()

# COMMAND ----------

# 1. Rank:

from pyspark.sql.window import Window
from pyspark.sql.functions import rank, desc

window_spec = Window.orderBy(desc('points'))
df.withColumn('rank', rank().over(window_spec)).show()

# The rank was determined based on the points of drivers, as you can see drivers Nico and Kimi scored 6 points and ranked as 5 then the next rank driver will be ranked as 7.

# COMMAND ----------

# 2. Dense rank:

from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank, desc

window_spec = Window.orderBy(desc('points'))
df.withColumn('dense_rank', dense_rank().over(window_spec)).show()

# The rank was determined based on the points of drivers, as you can see drivers Nico and Kimi scored 6 points and ranked as 5 then the next rank driver will be ranked as 6.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Commonly used Analytic function -> lag() and lead()

# COMMAND ----------

# 1. Lead:

from pyspark.sql.window import Window
from pyspark.sql.functions import lead

window_spec = Window.orderBy("points")
df.withColumn("lead", lead("points", offset=1).over(window_spec)).show()

# COMMAND ----------

# 2. Lag:

from pyspark.sql.window import Window
from pyspark.sql.functions import lag

window_spec = Window.orderBy("points")
df.withColumn("lag", lag("points", offset=1).over(window_spec)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Aggregate functions

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import min, max

window_spec = Window.partitionBy("team").orderBy("team", "points")

df\
.withColumn("min_points", min("points").over(window_spec))\
.withColumn("max_points", max("points").over(window_spec))\
.show()