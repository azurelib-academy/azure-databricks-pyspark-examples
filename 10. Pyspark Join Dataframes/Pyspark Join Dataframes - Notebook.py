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
# MAGIC #### Pyspark Join Dataframes in Azure Databricks

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

# 1. Employee Dataset
emp_data = [
    (1,"Bel",2),
    (2,"Thornie",1),
    (3,"Chris",2),
    (4,"Aloysia",2),
    (5,"Dorena",5)
]

emp_df = spark.createDataFrame(data = emp_data, schema = ["emp_id", "emp_name", "dept_id"])
emp_df.printSchema()
emp_df.show(truncate=False)

# COMMAND ----------

# 2. Department Dataset
dept_data = [
    (1, "IT"),
    (2, "HR"),
    (3, "Sales"),
    (4, "Marketing")
]

dept_df = spark.createDataFrame(data = dept_data, schema = ["dept_id", "dept_name"])
dept_df.printSchema()
dept_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### b) Create PySpark DataFrame by reading files

# COMMAND ----------

# replace the file_path with the source file location which you have downloaded.

emp_df_2 = spark.read.format("csv").option("header", True).load("employee_file_path")
emp_df_2.printSchema()

dept_df_2 = spark.read.format("csv").option("header", True).load("department_file_path")
dept_df_2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Note: Here, I will be using the manually created dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Common join types

# COMMAND ----------

# 1. Inner join
emp_df.join(dept_df, "dept_id", "inner").show(truncate=False)

# 2. Left Join: left, left_outer, leftouter join
emp_df.join(dept_df, emp_df.dept_id == dept_df.dept_id, "left").show(truncate=False)

# 3: Right join: right, right_outer, rightouter join
emp_df.join(dept_df, "dept_id", "right").show(truncate=False)

# 4. Outer join: outer, full, fullouter join
emp_df.join(dept_df, "dept_id", "outer").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Semi Join

# COMMAND ----------

emp_df.join(dept_df, "dept_id", "semi").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Anti Join

# COMMAND ----------

emp_df.join(dept_df, "dept_id", "anti").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4. Cross Join

# COMMAND ----------

emp_df.crossJoin(dept_df).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 5. Self Join

# COMMAND ----------

from pyspark.sql.functions import col

emp_df.alias("emp1") \
.join(emp_df.alias("emp2"), col("emp1.dept_id") == col("emp2.emp_id"), "left") \
.show()