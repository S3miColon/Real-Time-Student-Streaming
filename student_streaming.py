from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("RealTimeStudentProcessing") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Student data schema
schema = StructType([
    StructField("student_id", IntegerType()),
    StructField("student_name", StringType()),
    StructField("subject", StringType()),
    StructField("marks", IntegerType()),
    StructField("event_time", StringType())
])

# Read data from Kafka
kafka_data = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "student_data") \
    .option("startingOffsets", "latest") \
    .load()

# Convert Kafka JSON messages
json_data = kafka_data.select(
    from_json(col("value").cast("string"), schema).alias("data")
)

students = json_data.select("data.*")

# Process student records
processed_data = students.withColumn(
    "result",
    col("marks").cast("int")
)

processed_data = processed_data.withColumn(
    "status",
    __import__("pyspark").sql.functions.when(
        col("marks") >= 40, "PASS"
    ).otherwise("FAIL")
)

# Display real-time results
query = processed_data.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
