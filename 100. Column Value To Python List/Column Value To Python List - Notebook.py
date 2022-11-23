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
# MAGIC #### Column Value To Python List in Azure Databricks

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
    (1,"Jacynth","DL"),
    (2,"Rand","TN"),
    (3,"Marquita","DL"),
    (4,"Rodrick","KL"),
    (5,"Ingram","TN")
]

df = spark.createDataFrame(data, schema=["id","name","state"])
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
# MAGIC ##### 1) Column to List by specifying column index position

# COMMAND ----------

df.rdd.map(lambda column: column[2]).collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2) Column to List by specifying column name

# COMMAND ----------

from pyspark.sql.functions import collect_list

# Method 1: Using RDD
df.rdd.map(lambda column: column.state).collect()

# Method 2: Using DataFrame
column_row_values = df.select("state").collect()
state_list = list(map(lambda row: row.state, column_row_values))
print(list(state_list))

# Method 3:
df.select(collect_list("state").alias("states")).collect()[0]["states"]

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3) Column to List using flatmap

# COMMAND ----------

df.select("state").rdd.flatMap(lambda state: state).collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4) Column to list using pandas

# COMMAND ----------

states = df.toPandas()['state']
print(list(states))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 5) Getting column in row type

# COMMAND ----------

from pyspark.sql.functions import col

# Method 1:
df.select("state").collect()

# Method 2:
df.select(df.state).collect()

# Method 3:
df.select(col("state")).collect()

# Method 4:
df.select(df["state"]).collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 6) convert multiple column to list

# COMMAND ----------

pandas_df = df.toPandas()
names = list(pandas_df["name"])
states = list(pandas_df["state"])

print(f"Name: {names}")
print(f"States: {states}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 7) Remove duplicates after converting column values to list

# COMMAND ----------

from pyspark.sql.functions import collect_set

# Method 1:
row_states = df.rdd.map(lambda column: column.state).collect()
print(list(set(row_states)))

# Method 2:
row_states = df.select("state").collect()
unique_states = list(set(map(lambda row: row.state, row_states)))
print(unique_states)

# Method 3:
df.select(collect_set("state").alias("state")).collect()[0]["state"]

# COMMAND ----------

