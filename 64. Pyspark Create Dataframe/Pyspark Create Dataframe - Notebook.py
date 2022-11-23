# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark Create Dataframe in Azure Databricks

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
# MAGIC ##### Create RDD

# COMMAND ----------

columns = ["id","Name","Age"]
students_data = [
    (1, "Arjun", 23),
    (2, "Sandhiya", 25),
    (3, "Ranjith", 27)
]

rdd = sc.parallelize(students_data)
rdd.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Creating DataFrame from RDD

# COMMAND ----------

# 1. Using toDF() function

# a) without column name
df1 = rdd.toDF()
df1.printSchema()
df1.show()

# b) Using toDF() with column name
df2 = rdd.toDF(schema=columns)
df2.printSchema()
df2.show()

# COMMAND ----------

# 2. Using createDataFrame() function

# a) without column name
df3 = spark.createDataFrame(rdd)
df3.printSchema()
df3.show()

# b) with column name
df4 = spark.createDataFrame(rdd, schema=columns)
df4.printSchema()
df4.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Creating DataFrame from List of Collections

# COMMAND ----------

# 1. From list
list_data = [
    ["Arun", "Kumar", 13], 
    ["Janani", "Shree", 25]]

spark.createDataFrame(list_data, schema=["first_name", "last_name", "age"]).show()

# COMMAND ----------

# 2. From Tuple
tuple_data = [
    ("Charan", "Nagesh", 32), 
    ["Indhu", "Madhi", 26]]

spark.createDataFrame(tuple_data, schema=["first_name", "last_name", "age"]).show()

# COMMAND ----------

# 3. From key value pair -> unordered columns -> Therefore, we can't specify a defined column name
dict_data = [
    {'first_name': 'Balaji', 'last_name': 'Pandian', 'age': 46},
    {'first_name': 'Elango', 'last_name': 'Dharan', 'age': 41}
]
spark.createDataFrame(dict_data).show()

# COMMAND ----------

# 4. Using Row
from pyspark.sql import Row

row_data = [
    Row("Nithesh", "Khan", 14), 
    Row("Akash", "Sharma", 14)]

spark.createDataFrame(row_data, schema=["first_name", "last_name", "age"]).show()

# COMMAND ----------

# 5. Using Row with key value arguments
from pyspark.sql import Row

row_kv_data = [
    Row(first_name="Manik", last_name="Basha", age=54),
    Row(first_name="Vijay", last_name="Antony", age=36)
]
spark.createDataFrame(row_kv_data).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Creating DataFrame from Data Source

# COMMAND ----------

# 1. From CSV file
spark.read.csv(r"C:\Users\USER\Desktop\sample_csv.csv")

# 2. From Text file
spark.read.text(r"C:\Users\USER\Desktop\sample_text.txt")

# 3. From JSON file
spark.read.json(r"C:\Users\USER\Desktop\sample_json.json")

# 4. From Parquet file
spark.read.parquet(r"C:\Users\USER\Desktop\sample_parquet.parquet")