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
# MAGIC #### Pyspark Replace Nulls in Azure Databricks

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
# MAGIC ##### 1. Create manual PySpark DataFrame

# COMMAND ----------

data = [
    ("Duel in the Sun",None,None,7.0),
    ("Tom Jones",None,None,7.0),
    ("Oliver!","Musical","Sony Pictures",7.5),
    ("To Kill A Mockingbird",None,"Universal",8.4),
    ("Tora, Tora, Tora",None,None,None)
]

df = spark.createDataFrame(data, schema=["title", "genre", "distributor", "imdb_rating"])
df.printSchema()
df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Create PySpark DataFrame by reading files

# COMMAND ----------

# replace the file_path with the source file location which you have downloaded.

df_2 = spark.read.format("csv").option("inferSchema", True).option("header", True).load("file_path")
df_2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Note: Here, I will be using the manually created dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Replacing the null value of the entire column using fill() & fillna()

# COMMAND ----------

# Replace only the numberic columns
df.na.fill(0).show()
df.fillna(0).show()

# Replace only the string columns
df.na.fill("unknown").show()
df.fillna("unknown").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Replacing the null value of the selected column using fill() & fillna()

# COMMAND ----------

# 1. Changing the null values of 'genre' column to 'unknown-genre'
df.na.fill(value="unknown-genre", subset=["genre"]).show()
df.fillna(value="unknown-genre", subset=["genre"]).show()

# 2. Changing the null values of 'distributor' column to 'unknown-distributor'
df.na.fill(value="unknown-distributor", subset=["distributor"]).show()
df.fillna(value="unknown-distributor", subset=["distributor"]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Replacing the null value of the selected columns with different values

# COMMAND ----------

# Changing the null of selected columns with different values
df.na.fill({"genre": "unknown-genre", "distributor": "unknown-distributor"}).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4. Replacing the null value with aggregating value

# COMMAND ----------

from pyspark.sql.functions import avg

avg_rating = df.select(avg("imdb_rating").alias("avg_rating")).collect()[0]["avg_rating"]
print(f"The average IMDB rating is: {avg_rating}")

df.fillna(value=avg_rating, subset=["imdb_rating"]).show()
# Note you can't replace null value of numeric column with string value.