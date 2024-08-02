# Databricks notebook source
# MAGIC %sql
# MAGIC Use catalog testhexa;
# MAGIC Create schema bronze;
# MAGIC use bronze

# COMMAND ----------

from pyspark.sql.types import *
users_schema=StructType([StructField("Id", IntegerType()),
                         StructField("Name", StringType()),
                         StructField("Gender", StringType()),
                         StructField("Salary", IntegerType()),
                         StructField("Country", StringType()),
                         StructField("Date", StringType())
]) 

# COMMAND ----------

(
spark
.readStream
.schema(users_schema)
.csv("dbfs:/mnt/hexawaredatabricks/raw/stream_in/",header=True)
.writeStream
.option("checkpointLocation","dbfs:/mnt/hexawaredatabricks/raw/checkpoint/SANDIP/stream")
.trigger(once=True)
.table("testhexa.bronze.stream")
)

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from testhexa.bronze.stream

# COMMAND ----------

(spark
.readStream
.format("cloudFiles")
.option("cloudFiles.format","csv")
.option("cloudFiles.inferColumnTypes",True)
.option("cloudFiles.schemaLocation","dbfs:/mnt/hexawaredatabricks/raw/schemalocation/sandip/autoloader")
.load("dbfs:/mnt/hexawaredatabricks/raw/stream_in/")
.writeStream
.option("checkpointLocation","dbfs:/mnt/hexawaredatabricks/raw/checkpoint/sandip/autoloader")
.trigger(once=True)
.table("testhexa.bronze.autoloader")
)

# COMMAND ----------

(spark
.readStream
.format("cloudFiles")
.option("cloudFiles.format","csv")
.option("cloudFiles.inferColumnTypes",True)
.option("cloudFiles.schemaEvolutionMode","addNewColumns")
.option("cloudFiles.schemaLocation","dbfs:/mnt/hexawaredatabricks/raw/schemalocation/sandip/autoloader")
.load("dbfs:/mnt/hexawaredatabricks/raw/stream_in/")
.writeStream
.option("checkpointLocation","dbfs:/mnt/hexawaredatabricks/raw/checkpoint/sandip/autoloader")
.option("mergeSchema",True)
.trigger(once=True)
.table("testhexa.bronze.autoloader")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from testhexa.bronze.autoloader

# COMMAND ----------

df=spark.read.json("dbfs:/mnt/hexawaredatabricks/raw/input_files/adobe_sample.json")
df.display()

# COMMAND ----------

df=spark.read.json("dbfs:/mnt/hexawaredatabricks/raw/input_files/adobe_sample.json",multiLine=True)
df.display()

# COMMAND ----------

from pyspark.sql.functions import *
df1=df.withColumn("topping",explode("topping"))\
# .withColumn("topping_id",col("topping")[0])\
# .withColumn("topping_type",col("topping.type"))\
# .drop("topping")
df1.display()

# COMMAND ----------

from pyspark.sql.functions import *
df1=df.withColumn("batters",explode("batters.batter"))\
.withColumn("batters_id",col("batters.id"))\
.withColumn("batters_type",col("batters.type"))\
.drop("batters")\
.withColumn("topping",explode("topping"))\
.withColumn("topping_id",col("topping.id"))\
.withColumn("topping_type",col("topping.type"))\
.drop("topping")

# COMMAND ----------

df1.display()

# COMMAND ----------


