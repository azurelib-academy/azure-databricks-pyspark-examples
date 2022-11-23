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
# MAGIC #### Pyspark Select Column in Azure Databricks

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
    (1,"Sascha","1998-09-03"),
    (2,"Lise","2008-09-17"),
    (3,"Nola","2008-08-23"),
    (4,"Demetra","1997-06-02"),
    (5,"Lowrance","2006-07-02")
]

df = spark.createDataFrame(data, schema=["id","name","dob"])
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
# MAGIC ##### 1. Selecting single column

# COMMAND ----------

from pyspark.sql.functions import col

# 1. Selecting columns using String
df.select("name").show()

# 2. Selecting columns using python Dot Notation
df.select(df.name).show()

# 3. Selecting columns using column name as Key
df.select(df["name"]).show()

# 4. Selecting columns using col() function
df.select(col("name")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Selecting multiple columns

# COMMAND ----------

from pyspark.sql.functions import col

# 1. Selecting columns using String
df.select("id", "name").show()

# 2. Selecting columns using python Dot Notation
df.select(df.id, df.name).show()

# 3. Selecting columns using column name as Key
df.select(df["id"], df["name"]).show()

# 4. Selecting columns using col() function
df.select(col("id"), col("name")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Selecting entire column

# COMMAND ----------

# 1. Selecting all columns using "*" symbol
df.select("*").show()

# 2. Selecting all columns list
df.select(["id", "name", "dob"]).show()

# 3. Selecting all columns using columns field
df.select(df.columns).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4. Selecting column by index

# COMMAND ----------

# 1. Selecting the first column
df.select(df.columns[0]).show()

# 2. Selecting all the columns from second column
df.select(df.columns[1:]).show()

# 3. Selecting the columns on every two steps
df.select(df.columns[::2]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 5. Selecting columns in reverse order

# COMMAND ----------

df.select(df.columns[::-1]).show()