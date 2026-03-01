import logging
import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Configuracion de Logging
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format='%(asctime)s | %(name)s | %(levelname)s | %(message)s'
)
logger = logging.getLogger("AtlanticOps-Spark")

def create_spark_session():
    spark = SparkSession.builder \
        .appName("AtlanticOps-Streaming") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "atlantic_admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "atlantic_password") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.sql.streaming.stopTimeout", "60s") \
        .config("spark.scheduler.mode", "FAIR") \
        .config("spark.sql.streaming.noDataMicroBatches.enabled", "false") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

PORT_LAT_MIN = 28.10
PORT_LAT_MAX = 28.18
PORT_LON_MIN = -15.45
PORT_LON_MAX = -15.39

def main():
    logger.info("=" * 60)
    logger.info("ATLANTIC-OPS: MARITIME STREAMING PIPELINE")
    logger.info("=" * 60)
    
    # 🛑 MAGIA ANTI-CRASH: Obligamos a Spark a esperar 20 segundos a que Kafka despierte
    # y el Productor cree el Topic antes de intentar leer.
    logger.info("⏳ Esperando 20 segundos a que Kafka y Producer se inicialicen...")
    time.sleep(20)
    
    spark = create_spark_session()
    logger.info("✅ Spark Session Iniciada")
    
    schema = StructType([
        StructField("mmsi", IntegerType()),
        StructField("ship_name", StringType()),
        StructField("lat", DoubleType()),
        StructField("lon", DoubleType()),
        StructField("speed", DoubleType()),
        StructField("heading", DoubleType()),
        StructField("status", StringType()),
        StructField("timestamp", StringType())
    ])

    # Leer de Kafka con tolerancias
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "vessel_positions") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 200) \
        .option("failOnDataLoss", "false") \
        .load()

    df_parsed = df_kafka.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    df_clean = df_parsed.filter(
        (col("lat").isNotNull()) & 
        (col("lon").isNotNull()) & 
        (col("lat") != 0) & 
        (col("lon") != 0)
    )

    df_processed = df_clean.withColumn(
        "in_port", 
        (col("lat") >= PORT_LAT_MIN) & 
        (col("lat") <= PORT_LAT_MAX) & 
        (col("lon") >= PORT_LON_MIN) & 
        (col("lon") <= PORT_LON_MAX)
    ).withColumn("ingestion_time", current_timestamp())

    query = df_processed.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "s3a://lakehouse/checkpoints/vessel_data_v2") \
        .option("path", "s3a://lakehouse/vessel_data_v2") \
        .trigger(processingTime='10 seconds') \
        .start()

    logger.info("PIPELINE ACTIVO - Destino: s3a://lakehouse/vessel_data_v2")
    
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        query.stop()
        spark.stop()

if __name__ == "__main__":
    main()