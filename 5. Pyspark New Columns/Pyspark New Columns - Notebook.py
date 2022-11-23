# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark New Columns in Azure Databricks

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
    (1,"Jeanie","fm",446833),
    (2,"Poul","m",597923),
    (3,"Jonis","m",405672),
    (4,"Nellie","fm",742674),
    (5,"Sibbie","fm",35890)
]

df = spark.createDataFrame(data, schema=["id", "name", "gender", "salary"])
df.printSchema()
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### b) Create PySpark DataFrame by reading files

# COMMAND ----------

# replace the file_path with the source file location which you have downloaded.

df_2 = spark.read.format("csv").option("header", True).load(file_path)
df_2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Note: Here, I will be using the manually created dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Adding new column

# COMMAND ----------

from pyspark.sql.functions import lit

df.withColumn('country', lit('India')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Changing existing column value

# COMMAND ----------

from pyspark.sql.functions import col, upper

df.withColumn('gender', upper(col('gender'))).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Changing existing column DataType

# COMMAND ----------

from pyspark.sql.functions import col

df.select("salary").printSchema()

df = df.withColumn("salary", col("salary").cast("Double"))  
df.select("salary").printSchema()
df.show(truncate=False)