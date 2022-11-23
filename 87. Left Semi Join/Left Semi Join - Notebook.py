# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Left Semi Join in Azure Databricks

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

# 1. Student Dataset
student_data = [
    (1,"Clara",2),
    (2,"Conny",3),
    (3,"Sallie",1),
    (4,"Magdalene",3),
    (5,"Palm",3)
]

std_df = spark.createDataFrame(student_data, schema=["id","name","dept_id"])
std_df.printSchema()
std_df.show(truncate=False)

# COMMAND ----------

# 2. Department Dataset

dept_data = [
    (1,"civil"),
    (2,"mechanical"),
    (3,"cse"),
    (4,"it"),
    (5,"ece")
]

dept_df = spark.createDataFrame(dept_data, schema=["id","name"])
dept_df.printSchema()
dept_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### b) Create PySpark DataFrame by reading files

# COMMAND ----------

# replace the file_paths with the source file location which you have downloaded.

std_df_2 = spark.read.format("csv").option("header", True).load(student_file_path)
std_df_2.printSchema()

dept_df_2 = spark.read.format("csv").option("header", True).load(department_file_path)
dept_df_2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Note: Here, I will be using the manually created dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1) Left Semi Join

# COMMAND ----------

# Method 1:
std_df.join(dept_df, std_df.dept_id == dept_df.id, "semi").show()

# Method 2:
std_df.join(dept_df, std_df.dept_id == dept_df.id, "leftsemi").show()

# Method 3:
std_df.join(dept_df, std_df.dept_id == dept_df.id, "left_semi").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2) SQL Left Anti Join

# COMMAND ----------

std_df.createOrReplaceTempView("student")
dept_df.createOrReplaceTempView("department")

spark.sql('''
    SELECT *
    FROM student AS std
    LEFT SEMI JOIN department AS dept
    ON std.dept_id = dept.id
''').show()