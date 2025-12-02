# Init and instantiate Spark Session
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Data Import
# get_device = spark.read.json("data/device_data.json")
spark.sql("""
    CREATE TEMPORARY VIEW vw_device
          USING org.apache.spark.sql.json
            OPTIONS (path "data/device_data.json")
    """)

spark.sql("""
    CREATE TEMPORARY VIEW vw_subscription
          USING org.apache.spark.sql.json
            OPTIONS (path "data/subscription_data.json")
    """)

print(spark.catalog.listTables())

# Select Data
spark.sql("""
    SELECT *
    FROM vw_device
    LIMIT 10;
    """).show()

spark.sql("""
    SELECT *
    FROM vw_subscription
    LIMIT 10;
    """).show()

# New DataFrame with SQL (join)
join_datasets = spark.sql("""
    SELECT
        d.device_id,
        d.device_type,
        s.subscription_type,
        s.start_date,
        s.end_date
    FROM vw_device d
    JOIN vw_subscription s
    ON d.device_id = s.device_id                   
    """)

join_datasets.show()
join_datasets.printSchema()
join_datasets.count()