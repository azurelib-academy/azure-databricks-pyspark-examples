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
# MAGIC #### Pyspark Partitionby() Function in Azure Databricks

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
    ("Olga Glatskikh","Russia",2004,"Rhythmic Gymnastics",1,0,0,1),
    ("Anna Pavlova","Russia",2004,"Swimming",0,0,2,2),
    ("Dan Izotov","Russia",2008,"Swimming",0,1,0,1),
    ("Katie Ledecky","United States",2012,"Swimming",1,0,0,1),
    ("Kyla Ross","United States",2008,"Gymnastics",1,0,0,1),
    ("Tasha Schwikert-Warren","United States",2000,"Gymnastics",0,0,1,1),
    ("Yang Yun","China",2000,"Gymnastics",0,0,1,1),
    ("Yang Yilin","China",2008,"Diving",1,0,2,3),
    ("Chen Ruolin","China",2008,"Diving",2,0,0,2)
]

df = spark.createDataFrame(data, schema=["Athlete","Country","Year","Sport","Gold","Silver","Bronze","Total"])
df.printSchema()
df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Create PySpark DataFrame by reading files

# COMMAND ----------

file_path = "/mnt/formulaoneadls/practice/olympic-athletes.csv"
# replace the file_path with the source file location which you have downloaded.

df_2 = spark.read.format("csv").option("inferSchema", True).option("header", True).load(file_path)
df_2.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Note: Here, I will be using the manually created dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Partition data by single column

# COMMAND ----------

sink_path = "/mnt/formulaoneadls/practice/partitionBy/partitionByYear/"

df.write.option("header", True) \
.partitionBy("Year") \
.format("csv").mode("overwrite") \
.save(sink_path)

for each_folder in dbutils.fs.ls(sink_path):
    print(each_folder.path)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2. Partition data by multiple column

# COMMAND ----------

sink_path = "/mnt/formulaoneadls/practice/partitionBy/partitionByYearAndCountry/"

df.write.option("header", True) \
.partitionBy(["Year", "Country"]) \
.format("csv").mode("overwrite") \
.save(sink_path)

# First layer of partitioning
for each_folder in dbutils.fs.ls(sink_path):
    print(each_folder.path)

print("----------------")

# Second layer of partitioning
for each_folder in dbutils.fs.ls(f"{sink_path}/Year=2008/"):
    print(each_folder.path)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3. Reading specific partition

# COMMAND ----------

year_2012_df = spark.read.format("csv").option("header", True) \
.csv("/mnt/formulaoneadls/practice/partitionBy/partitionByYear/Year=2012")

year_2012_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 4. Control Number of Partition File

# COMMAND ----------

sink_path = "/mnt/formulaoneadls/practice/partitionBy/partitionByCountry"

df.repartition(1) \
.write.partitionBy("Country") \
.option("header", True) \
.format("csv").mode("overwrite") \
.save(sink_path)

for each_folder in dbutils.fs.ls(f"{sink_path}/Country=Russia"):
    if each_folder.name[0] == "p":
        print(each_folder.name)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 5. Control Number of Records per Partition File

# COMMAND ----------

sink_path = "mnt/formulaoneadls/practice/partitionBy/partitionByCountry_2"

# Write file with maximum of two records per partition
df.write.option("header", True) \
.option("maxRecordsPerFile", 2) \
.partitionBy("Country") \
.format("csv").mode("overwrite") \
.save(f"/{sink_path}")

# Check number of records in each partition
for each_folder in dbutils.fs.ls(f"/{sink_path}/Country=Russia"):
    if each_folder.name[0] == "p":
        with  open(f"/dbfs/{sink_path}/Country=Russia/{each_folder.name}") as file:
            for line in file:
             print(line.strip())
        print("-----------")

# COMMAND ----------

