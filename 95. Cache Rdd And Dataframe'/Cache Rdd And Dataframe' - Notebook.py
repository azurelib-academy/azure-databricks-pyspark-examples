# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cache Rdd And Dataframe' in Azure Databricks

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
# MAGIC ##### 1) Problem statement

# COMMAND ----------

from pyspark.sql.functions import col

data = [
    ("Mamie","Treharne"),
    ("Erv","Colam"),
    ("Daren","Salliss"),
    ("Vania","Laundon"),
    ("Jay","Kees")
]

df1 = spark.createDataFrame(data, schema=["f_name","l_name"]) # <- (1)

df2 = df1.filter(col("l_name").endswith("s")) # <- (2)
df2.show()

df3 = df2.filter(col("f_name").startswith("D")) # <- (3)
df3.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2) Caching DataFrame

# COMMAND ----------

from pyspark.sql.functions import col

data = [
    ("Mamie","Treharne"),
    ("Erv","Colam"),
    ("Daren","Salliss"),
    ("Vania","Laundon"),
    ("Jay","Kees")
]

df1 = spark.createDataFrame(data, schema=["f_name","l_name"]).cache() # <- (1)

df2 = df1.filter(col("l_name").endswith("s")).cache() # <- (2)
df2.show()

df3 = df2.filter(col("f_name").startswith("D"))
df3.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3) Caching RDD

# COMMAND ----------

rdd1 = df1.rdd.cache() # <- (1)

rdd2 = rdd1.filter(lambda tup: tup[1][-1] == 's') # <- (2)
rdd2.collect()

# COMMAND ----------

rdd3 = rdd2.filter(lambda tup: tup[0][0] == 'D') # <- (3)
rdd3.collect()