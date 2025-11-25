import os

# ======================
# FIX: PYSPARK PYTHON
# ======================
os.environ["PYSPARK_PYTHON"] = r"C:\Users\Maryna\AppData\Local\pypoetry\Cache\virtualenvs\goit-de-fp-2ia1juPn-py3.11\Scripts\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\Maryna\AppData\Local\pypoetry\Cache\virtualenvs\goit-de-fp-2ia1juPn-py3.11\Scripts\python.exe"

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, current_timestamp, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from configs import kafka_config, jdbc_config

jdbc_url = jdbc_config["jdbc_url"]
jdbc_user = jdbc_config["jdbc_user"]
jdbc_password = jdbc_config["jdbc_password"]
jdbc_jar = jdbc_config["jdbc_jar"]

input_topic = "athlete_event_results"
output_topic = "athlete_stats"

# -----------------------------
# CHECK JDBC DRIVER
# -----------------------------
if not os.path.exists(jdbc_jar):
    print(f"❌ Error: JDBC driver not found: {jdbc_jar}")
else:
    print(f"✅ JDBC driver loaded: {jdbc_jar}")

# -----------------------------
# SPARK SESSION
# -----------------------------
print("Init Spark session...")
spark = SparkSession.builder \
    .appName("AthleteBioAndResultsStreamingPipeline") \
    .master("local[*]") \
    .config("spark.jars", jdbc_jar) \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.driver.host", "127.0.0.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ------------------------------------------------
# STAGE 1 — READ BIO DATA
# ------------------------------------------------
print("stage 1: Read bio data from MySQL")

athlete_bio_df = spark.read.format('jdbc').options(
    url=jdbc_url,
    driver='com.mysql.cj.jdbc.Driver',
    dbtable="athlete_bio",
    user=jdbc_user,
    password=jdbc_password
).load()

athlete_bio_df.printSchema()
athlete_bio_df.show(5)

# ------------------------------------------------
# STAGE 2 — CLEAN BIO DATA
# ------------------------------------------------

print("Stage 2: filter bio data")

bio_clean = athlete_bio_df \
    .withColumn("height_float", expr("try_cast(height AS float)")) \
    .withColumn("weight_float", expr("try_cast(weight AS float)"))

filtered_bio_df = bio_clean.filter(
    col("height_float").isNotNull() &
    col("weight_float").isNotNull()
)

print("Filtered Bio Data Count:", filtered_bio_df.count())
filtered_bio_df.show(5)

# ------------------------------------------------
# STAGE 3A — READ EVENT RESULTS + WRITE TO KAFKA
# ------------------------------------------------

print("Stage 3a: Read results from MySQL")
event_results_df = spark.read.format('jdbc').options(
    url=jdbc_url,
    driver='com.mysql.cj.jdbc.Driver',
    dbtable="athlete_event_results",
    user=jdbc_user,
    password=jdbc_password
).load()

event_results_df.show(5)

print("Write to Kafka...")

event_results_df.selectExpr("to_json(struct(*)) AS value") \
    .write \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0]) \
    .option("kafka.security.protocol", kafka_config['security_protocol']) \
    .option("kafka.sasl.mechanism", kafka_config['sasl_mechanism']) \
    .option("kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.plain.PlainLoginModule required '
            f'username="{kafka_config["username"]}" password="{kafka_config["password"]}";') \
    .option("topic", input_topic) \
    .save()

print("Data has been written to Kafka topic.")

# ------------------------------------------------
# STAGE 3B — READ STREAM FROM KAFKA
# ------------------------------------------------

event_schema = StructType([
    StructField("edition", StringType(), True),
    StructField("edition_id", IntegerType(), True),
    StructField("country_noc", StringType(), True),
    StructField("sport", StringType(), True),
    StructField("event", StringType(), True),
    StructField("result_id", IntegerType(), True),
    StructField("athlete", StringType(), True),
    StructField("athlete_id", IntegerType(), True),
    StructField("pos", StringType(), True),
    StructField("medal", StringType(), True),
    StructField("isTeamSport", StringType(), True)
])

print("Stage 3b: Setup Kafka stream")

kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0]) \
    .option("kafka.security.protocol", kafka_config['security_protocol']) \
    .option("kafka.sasl.mechanism", kafka_config['sasl_mechanism']) \
    .option("kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.plain.PlainLoginModule required '
            f'username="{kafka_config["username"]}" password="{kafka_config["password"]}";') \
    .option("subscribe", input_topic) \
    .option("startingOffsets", "earliest") \
    .load()

parsed_stream = kafka_stream \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), event_schema).alias("data")) \
    .select("data.*") \
    .withColumn("athlete_id", col("athlete_id").cast("int"))

# ------------------------------------------------
# FOREACH BATCH
# ------------------------------------------------

def process_batch(batch_df, batch_id):
    print(f"\n====================")
    print(f"Batch processing {batch_id}")

    # FIX: Cannot use rdd.isEmpty() in streaming
    if batch_df.head(1) == []:
        print("Batch is empty — skipping.")
        return

    print(f"Batch rows: {batch_df.count()}")
    batch_df.show(5)

    print("Stage 4: Join with bio data")
    joined_df = batch_df.join(
        filtered_bio_df,
        "athlete_id",
        "inner"
    )

    print("Joined rows:", joined_df.count())
    joined_df.show(5)

    print("Stage 5: Aggregation")
    stats_df = joined_df.groupBy(
        "sport",
        "medal",
        "sex",
        "country_noc"
    ).agg(
        avg("height_float").alias("avg_height"),
        avg("weight_float").alias("avg_weight")
    ).withColumn("timestamp", current_timestamp())

    stats_df.show()

    # Write to Kafka
    print("Stage 6a: Write to Kafka")
    stats_df.selectExpr("to_json(struct(*)) AS value") \
        .write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0]) \
        .option("topic", output_topic) \
        .save()

    print("Stage 6b: Write to MySQL")
    stats_df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "maryna_kindras.athlete_status") \
        .option("user", jdbc_user) \
        .option("password", jdbc_password) \
        .mode("append") \
        .save()

    print(f"✔ Batch {batch_id} completed")

# ------------------------------------------------
# RUN STREAM
# ------------------------------------------------

print("Run stream...")

query = parsed_stream.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("update") \
    .start()

query.awaitTermination()
