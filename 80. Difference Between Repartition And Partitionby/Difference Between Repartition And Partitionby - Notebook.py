# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Difference Between Repartition And Partitionby in Azure Databricks

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
    (1,"Ricki","Russia"),
    (2,"Madelaine","Russia"),
    (3,"Neil","Brazil"),
    (4,"Lock","Brazil"),
    (5,"Prissie","India"),
    (6,"Myrilla","India"),
    (7,"Correna","USA"),
    (8,"Ricki","USA")
]

df = spark.createDataFrame(data, schema=["id","name","country"])
df.printSchema()
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### b) Create PySpark DataFrame by reading files

# COMMAND ----------

file_path = "/mnt/practice/people_by_country.csv"
# replace the file_path with the source file location which you have downloaded.

df_2 = spark.read.format("csv").option("header", True).load(file_path)
df_2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Note: Here, I will be using the manually created dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1) repartition(numberOfPartitions)

# COMMAND ----------

df1 = df.repartition(4)

# replace the save_location with your saving location
df1.write.format("csv").mode("overwrite").save("save_location")

# Check number of file has been written
for record in dbutils.fs.ls("saved_location"):
    # The written file name always starts with 'p'
    if record.name.startswith("p"):
        print(record.name)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2) repartition(numberOfPartitions, columns)

# COMMAND ----------

df2 = df.repartition(2, "country")

# replace the save_location with your saving location
df2.write.format("csv").mode("overwrite").save("save_location")

# Check number of file has been written
for record in dbutils.fs.ls("saved_location"):
    # The written file name always starts with 'p'
    if record.name.startswith("p"):
        print(record.name)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3) partitionBy()

# COMMAND ----------

df.write.partitionBy("country").format("csv").mode("overwrite").save("save_location")

# Check number of file has been written
for record in dbutils.fs.ls("saved_location"):
    if record.name.startswith("country"):
        print(record.name)