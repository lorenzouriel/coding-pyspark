# Init
from pyspark.sql import SparkSession

builder = SparkSession.builder.appName("app")
builder = builder.config("spark.sql.execution.arrow.pyspark.enabled", "true")
spark = builder.getOrCreate()
print(spark)

# pandas on sparl
import pyspark.pandas as ps

# Read files
get_device = ps.read_json("data/device.json")
get_subscription = ps.read_json("data/subscription.json")

print(get_device)
print(get_subscription)

get_device.info()
get_subscription.info()

# Show schema
get_device.print_schema()
get_subscription.print_schema()

# Get Plan
get_device.spark.explain(mode="formatted")
get_subscription.spark.explain(mode="formatted")