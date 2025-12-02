# Import Libraries and Init Spark Session
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Load data
df_device = spark.read.json("data/device_data.json")
# df_device = spark.read.json("data/*.json")
# df_device = spark.read.json("data/*.json")
# df_device.show()

# Schema Inspection
df_device.printSchema()

# Columns Inspection
print(df_device.columns)

# Row Count
print(df_device.count().show())

# Select Columns
print(df_device.select("manufacturer", "model", "platform"))
df_device.select("manufacturer", "model", "platform").show()
df_device.selectExpr("manufacturer", "model", "platform as type").show()

# Filter
df_device.filter(df_device.manufacturer == "Xiaomi").show()

# GroupBy
df_device.groupBy("manufacturer").count().show()
df_device.groupBy("manufacturer").count().orderBy("manufacturer").show()

df_grouped_manufacturer = df_device.groupBy("manufacturer").count()
df_grouped_manufacturer.show()



# Stop Spark Session
spark.stop()