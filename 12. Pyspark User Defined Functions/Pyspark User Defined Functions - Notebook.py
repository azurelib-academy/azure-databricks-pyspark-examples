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
# MAGIC #### Pyspark User Defined Functions in Azure Databricks

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
    (1,"Nitheesh",22,"m","856 324 321"),
    (2,"Ranveer",21,"Male","7585543210"),
    (3,"Kritisha",24,"FM","9876543210 "),
    (4,"Madhu",26,"female","987 657 8954")    
]

df = spark.createDataFrame(data, schema=["id","name","age","gender","mobile"])
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
# MAGIC ##### 1. Create and use a UDF

# COMMAND ----------

from pyspark.sql.functions import udf

# 1. Create function
def reformat_gender_udf(gender: str) -> str:
    first_char = gender[0].lower()
    output = "Male"
    if first_char == "f":
        output = "Female"
    return output

# 2. Register function using udf() method
reformat_gender = udf(reformat_gender_udf)

# 3. Apply function on top of Dataframe columns
df.withColumn("gender", reformat_gender("gender")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Using UDF on PySpark SQL

# COMMAND ----------

from pyspark.sql.types import StringType

# Register the user defined function
spark.udf.register("reformat_gender", reformat_gender_udf, StringType())

df.createOrReplaceTempView("sample")

spark.sql("""
SELECT id, name, age, reformat_gender(gender) AS gender, mobile
FROM sample
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Using PySpark in-built functions

# COMMAND ----------

from pyspark.sql.functions import col, lower, when, substring

df.withColumn("gender", 
              when(substring(lower(col("gender")), 1, 1) == "m", "Male")\
              .otherwise("Female")
             ).show()

# COMMAND ----------

from pyspark.sql.functions import col, trim, split, concat_ws, length, when, udf

df.withColumn("mobile", 
             when(length(concat_ws("", split(trim(col("mobile")), " "))) != 10, None)\
              .otherwise(concat_ws("", split(trim(col("mobile")), " ")))
             ).show()