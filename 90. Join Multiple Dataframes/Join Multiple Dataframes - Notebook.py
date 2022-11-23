# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Join Multiple Dataframes in Azure Databricks

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
    (1,"Karee",1,"A"),
    (2,"Jobie",2,"B"),
    (3,"Kyle",3,"A"),
    (4,"Georges",1,"B"),
    (5,"Tracey",2,"A")
]

std_df = spark.createDataFrame(student_data, schema=["id","name","dept_id","section"])
std_df.printSchema()
std_df.show(truncate=False)

# COMMAND ----------

# 2. Department Dataset
dept_data = [
    (1,"civil","A",301),
    (1,"civil","B",302),
    (2,"mech","A",401),
    (2,"mech","B",402),
    (3,"ece","A",501),
    (3,"ece","B",502)
]

columns = ["id","name","section","hall_no"]
dept_df = spark.createDataFrame(data = dept_data, schema=columns)
dept_df.printSchema()
dept_df.show(truncate=False)

# COMMAND ----------

# Hall details
hall_data = [  
    (301,1),
    (302,1),
    (401,2),
    (402,2),
    (501,3),
    (502,3)
]

hall_df = spark.createDataFrame(data=hall_data, schema=["hall_no","floor_no"])
hall_df.printSchema()
hall_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### b) Create PySpark DataFrame by reading files

# COMMAND ----------

# replace the file_paths with the source file location which you have downloaded.

std_df_2 = spark.read.format("csv").option("header", True).load(student_file_path)
std_df_2.printSchema()

dept_df_2 = spark.read.format("csv").option("header", True).load(department_file_path)
dept_df_2.printSchema()

hall_df_2 = spark.read.format("csv").option("header", True).load(hall_file_path)
hall_df_2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Note: Here, I will be using the manually created dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1) Joining two dataframes

# COMMAND ----------

dept_df.join(hall_df, dept_df.hall_no == hall_df.hall_no, "inner").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2) Eliminate duplicate column while joining DataFrames

# COMMAND ----------

# Method 1:
dept_df.join(hall_df, "hall_no", "inner").show()

# Method 2:
dept_df.join(hall_df, ["hall_no"], "inner").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3) Joining multiple DataFrames using chaining

# COMMAND ----------

std_df \
.join(dept_df, std_df.dept_id == dept_df.id, "inner") \ # First chain
.join(hall_df, "hall_no", "inner").show(5) # Second chain

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4) Multiple join conditions

# COMMAND ----------

multiple_join_condition  = (std_df.dept_id == dept_df.id) & (std_df.section == dept_df.section)
std_dept_df = std_df.join(dept_df, multiple_join_condition , how="inner")
std_dept_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 5) Multiple join conditions using where or filter function

# COMMAND ----------

first_condition = std_df.dept_id == dept_df.id
second_condition = std_df.section == dept_df.section

# 1. Multiple join using where
std_dept_df_2 = std_df.join(dept_df, first_condition, how="inner") # <- first condition in join()
std_dept_df_2 = std_dept_df_2.where(second_condition) # <- Second condition in where()
std_dept_df_2.show()

# 2. Multiple join using where
std_dept_df_3 = std_df.join(dept_df, first_condition, how="inner") # <- first condition in join()
std_dept_df_3 = std_dept_df_2.filter(second_condition) # <- Second condition in filter()
std_dept_df_3.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 6) Multiple Join SQL expression

# COMMAND ----------

std_df.createOrReplaceTempView("student")
dept_df.createOrReplaceTempView("department")
hall_df.createOrReplaceTempView("hall")

spark.sql('''
    SELECT *
    FROM student AS std
    JOIN department AS dept
    ON std.dept_id = dept.id AND std.section = dept.section
    JOIN hall ON hall.hall_no = dept.hall_no
''').show()