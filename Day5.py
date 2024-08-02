# Databricks notebook source
dbutils.fs.mount(
  source = "wasbs://hwcontainer1@hwstorageac.blob.core.windows.net",
  mount_point = "/mnt/hwstorageac/hwcontainer1",
  extra_configs = {"fs.azure.account.key.hwstorageac.blob.core.windows.net":"ghkkkkkkkkkg"})

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/hwstorageac/hwcontainer1/formula1/

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from json.`dbfs:/mnt/hwstorageac/hwcontainer1/formula1/drivers.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema formula1

# COMMAND ----------

# MAGIC %sql
# MAGIC create table formula1.constructor as 
# MAGIC select * from json.`dbfs:/mnt/hwstorageac/hwcontainer1/formula1/drivers.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from formula1.constructor

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from csv.`dbfs:/mnt/hwstorageac/hwcontainer1/formula1/circuits.csv`

# COMMAND ----------

from pyspark.sql.functions import *
def add_num(a, b):
    return a + b

# COMMAND ----------

from pyspark.sql.types import IntegerType
spark.udf.register('ADD_NUMBERS',add_num,IntegerType())

# COMMAND ----------

from pyspark.sql.types import IntegerType
ADD_NUMBERS=udf(add_num,IntegerType())

# COMMAND ----------

schema=['no1','no2']
df=spark.createDataFrame(((1,2),),schema=schema)
df=df.selectExpr('no1','no2','ADD_NUMBERS(no1,no2) as No3')
df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC

# COMMAND ----------

df=spark.read.json('dbfs:/mnt/hwstorageac/hwcontainer1/formula1/pit_stops.json',multiLine=True)
df.display()

# COMMAND ----------

df=spark.read.csv('dbfs:/mnt/hwstorageac/hwcontainer1/formula1/races.csv',header=True,inferSchema=True)
df=df.withColumnRenamed('raceId','race_id')\
.withColumnRenamed('circuitId','circuit_id')\
.withColumn('current_timestamp',current_timestamp())\
.withColumn('path',input_file_name())\
.drop('url')
df.display()


# COMMAND ----------

catlog='testhexa'
schema='formula1'

# COMMAND ----------

df.write.mode('overwrite').saveAsTable(f"{catlog}.{schema}.races")

# COMMAND ----------


