# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark Coumn Class in Azure Databricks

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
    ("Talbert","Taysbil","21-07-1992",44,"Male","6024 Onsgard Way","IL"),
    ("Dyana","Tomini","10-03-1963",59,"Female","082 Loomis Pass","TX"),
    ("Fredia","Cuschieri","03-03-1957",23,"Female","681 Mayer Lane","NV"),
    ("Kyle","Helleckas","08-12-1956",44,"Female","054 Manitowish Crossing","FL"),
    ("Linet","Stroyan","23-09-1996",12,"Female","8242 Sunfield Plaza","NC"),
    ("Nev","Borsi","04-01-1992",None,"Male","59 Utah Street","NY"),
    ("Ogdan","Baraja","04-01-1982",44,"Male","567 Oriole Park","FL"),
    ("Darcee","Draper","03-02-1979",None,"Female","5406 Monterey Terrace","AL"),
    ("Delainey","Caley","16-07-1975",47,"Male","83 Fremont Plaza","NC"),
    ("Siward","Calder","04-03-1958",None,"Male","22 Cody Trail","CA")
]

columns = ["f_name","l_name","dob","age","gender","street","state"]
df = spark.createDataFrame(data, schema=columns)
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### b) Create PySpark DataFrame by reading files

# COMMAND ----------

file_path = "/mnt/practice/column_function_data.json"
# replace the file_path with the source file location which you have downloaded.

df_2 = spark.read.format("csv").option("header", True).load(file_path)
df_2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Note: Here, I will be using the manually created dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC ##### a) Creating column object

# COMMAND ----------

from pyspark.sql.functions import lit

df.select("f_name", "age", lit("azurelib.com").alias("url")).show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### b) Accessing columns

# COMMAND ----------

from pyspark.sql.functions import col, column

# Method 1: Using column name as string
df.select("f_name").show(1)

# Method 2: Using dot notation
df.select(df.f_name).show(1)

# Method 3: using column name as key
df.select(df['f_name']).show(1)

# Method 4: Using col() function
df.select(col("f_name")).show(1)

# Method 5: Using column() function
df.select(column("f_name")).show(1)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### c) Column operations

# COMMAND ----------

from pyspark.sql.functions import col

# Using matematically operation
df.select("age",
  (col("age")+1).alias("age + 1"),
  (col("age")-1).alias("age - 1"),
  (col("age")*10).alias("age * 10"),
  (col("age")/10).alias("age / 10"),
  (col("age")%2).alias("age % 2")
).show(2)

# Using greater than operator
df.filter(col("age")<40).show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### d) Common column function

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1.Rename column and change column datatype

# COMMAND ----------

from pyspark.sql.functions import col

# Modifying column names
df.select(
    col("f_name").alias("first_name"),
    col("l_name").name("last_name")
).show(5, truncate=False)

# Modifying column datatype
df.select(
    col("age"),
    col("age").cast("INT"),
    col("age").astype("STRING")
).printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Extracting column values

# COMMAND ----------

# Substring
from pyspark.sql.functions import col

df \
.withColumn("date", col("dob").substr(1, 2)) \
.withColumn("month", col("dob").substr(4, 2)) \
.withColumn("year", col("dob").substr(7, 4)) \
.select("dob","date","month","year") \
.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Ordering column

# COMMAND ----------

# Substring
from pyspark.sql.functions import col

# Ascending
df.orderBy(col("age").asc()).show(5)
df.orderBy(col("age").asc_nulls_first()).show(5)
df.orderBy(col("age").asc_nulls_last()).show(5)

# Descending
df.orderBy(col("age").desc()).show(5)
df.orderBy(col("age").desc_nulls_first()).show(5)
df.orderBy(col("age").desc_nulls_last()).show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4. Bool expression

# COMMAND ----------

from pyspark.sql.functions import col

# Starts with
df.filter(col("f_name").startswith("D")).show(5)
# Ends with
df.filter(col("f_name").endswith("e")).show(5)
# Contains
df.filter(col("f_name").contains("ce")).show(5)
# Between
df.filter(col("age").between(40, 50)).show(5)
# Like
df.filter(col("l_name").like("T%")).show(5)
# isin
df.filter(col("state").isin(['IL','FL'])).show(5)
# isNull
df.filter(col("age").isNull()).show(5)
# isNotNull
df.filter(col("age").isNotNull()).show(5)