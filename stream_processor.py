from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("IoT Alerts") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "building_sensors") \
    .load()

schema = StructType([
    StructField("sensor_id", StringType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("timestamp", StringType())
])

parsed = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", to_timestamp("timestamp"))

agg = parsed \
    .withWatermark("timestamp", "10 seconds") \
    .groupBy(
        window("timestamp", "1 minute", "30 seconds")
    ) \
    .agg(
        avg("temperature").alias("avg_temp"),
        avg("humidity").alias("avg_hum")
    )

alerts = spark.read.csv("alerts_conditions.csv", header=True, inferSchema=True)

joined = agg.crossJoin(alerts)

filtered = joined.where(
    ((col("temperature_max") != -999) & (col("avg_temp") > col("temperature_max"))) |
    ((col("temperature_min") != -999) & (col("avg_temp") < col("temperature_min"))) |
    ((col("humidity_max") != -999) & (col("avg_hum") > col("humidity_max"))) |
    ((col("humidity_min") != -999) & (col("avg_hum") < col("humidity_min")))
)

output = filtered.selectExpr(
    "CAST(id AS STRING) AS key",
    "to_json(struct(*)) AS value"
)

query = output.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "alerts") \
    .option("checkpointLocation", "./checkpoints") \
    .start()

query.awaitTermination()
