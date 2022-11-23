# Databricks notebook source
# MAGIC %md
# MAGIC ##### Welcome  to Azurelib Academy By Deepak Goyal
# MAGIC ##### You can master the Databricks here: <a href='https://adeus.azurelib.com' target='_blank'>www.adeus.azurelib.com</a>
# MAGIC ##### Author of this blog: Arud Seka Berne S <a href="https://www.linkedin.com/in/arudsekaberne/" target="_blank" style="text-decoration:none;"><span style="padding:0.5px 2.5px;border-radius:2px;background:#0A66C2"><strong style="width:10px;font-family:'Poppins',sans-serif;;color:white">in</strong></span>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pyspark Row Class in Azure Databricks

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
# MAGIC ##### 1. Create row object using positional argument

# COMMAND ----------

from pyspark.sql import Row

row = Row("Alex", "IT", 1000)
row

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Create row object using named arguments

# COMMAND ----------

from pyspark.sql import Row

row = Row(name="Alex", dept="IT", salary=1000)
row

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Creating a row from another row object

# COMMAND ----------

from pyspark.sql import Row

emp = Row("name", "dept", "salary")
emp1 = emp("Alex", "IT", 1000)
emp2 = emp("Bruno", "Sales", 2000)
emp2

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4. Passing null values

# COMMAND ----------

from pyspark.sql import Row

# Method 1:
null_row_1 = Row("Alex", None, 1000)
print(null_row_1)

# Method 2:
null_row_2 = Row(name="Bruno", dept="Sales", salary=None)
print(null_row_2)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 5. Accessing row values

# COMMAND ----------

from pyspark.sql import Row

row = Row(name="Bruno", dept="Sales", salary=None)
# a) Using index position
print(row[0])
# b) Using like an attribute
print(row.dept)
# c) Using like a key
print(row['salary'])

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 6. Row methods

# COMMAND ----------

from pyspark.sql import Row

row = Row("Alex", "IT", 1000, 1000, None)

# Method 1:
print(row.count(1000))

# Method 2:
print(row.index(None))

# COMMAND ----------

row = Row(id=1, f_name="Shalini", l_name="Shree")
nested_row = Row(id=1, name=Row(f_name="Shalini", l_name="Shree"))

# Method 3:
print(row.asDict() == {'id':1,'f_name':'Shalini', 'l_name':'Shree'})
print(nested_row.asDict() == {'id':1,'name':Row(f_name="Shalini", l_name="Shree")})
print(nested_row.asDict(recursive=True) == {'id':1,'name':{'f_name':'Shalini', 'l_name':'Shree'}})

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 7. Using Row class on RDD

# COMMAND ----------

employee_data = [
    Row("Alex", "IT", 1000),
    Row("Bruno", "Sales", 2000)
]

rdd = sc.parallelize(employee_data)
rdd.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 8. Using Row class on DataFrame

# COMMAND ----------

employee_data = [
    Row(name="Alex", dept="IT", salary=1000),
    Row(name="Bruno", dept="Sales", salary=2000)
]

df = spark.createDataFrame(employee_data)
df.show()