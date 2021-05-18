from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, window, max
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

if __name__ == "__main__":

# Create spark session and set configurations, here we set yarn, because we want to run spark application on hadoop environment
    spark = SparkSession \
        .builder \
        .appName("Sliding Window Steram") \
        .master("yarn") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.shuffle.partitions", 1) \
        .getOrCreate()

# Define schema for kafka data
# Schema will be used during deserialization of kafka data
    stock_schema = StructType([
        StructField("CreatedTime", StringType()),
        StructField("Reading", DoubleType())
    ])

# Read data from kafka topic
    kafka_source_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "sensor") \
        .option("startingOffsets", "earliest") \
        .load()

# Deserialization
    value_df = kafka_source_df.select(col("key").cast("string").alias("SensorID"),
                                      from_json(col("value").cast("string"), stock_schema).alias("value"))

    #value_df.printSchema()

# Select both key and all values, convert time from string type to timestamp
    sensor_df = value_df.select("SensorID", "value.*") \
        .withColumn("CreatedTime", to_timestamp(col("CreatedTime"), "yyyy-MM-dd HH:mm:ss"))

# Aggregate to find maximum temperature for given window interval
# Set 5 minut slide
    agg_df = sensor_df \
        .withWatermark("CreatedTime", "30 minute") \
        .groupBy(col("SensorID"),
                 window(col("CreatedTime"), "15 minute", "5 minute")) \
        .agg(max("Reading").alias("MaxReading"))

# Create output dataset
    output_df = agg_df.select("SensorID", "window.start", "window.end", "MaxReading")

# Write result to console in update mode
    window_query = output_df.writeStream \
        .format("console") \
        .outputMode("update") \
        .option("checkpointLocation", "Sensor/chk-point-dir") \
        .trigger(processingTime="1 minute") \
        .start()

    window_query.awaitTermination()
