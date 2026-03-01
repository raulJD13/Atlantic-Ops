import logging
import sys
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

# Constantes del Puerto de La Luz
PORT_LAT_MIN = 28.10
PORT_LAT_MAX = 28.18
PORT_LON_MIN = -15.45
PORT_LON_MAX = -15.39

def main():
    logger.info("=" * 60)
    logger.info("ATLANTIC-OPS: MARITIME STREAMING PIPELINE")
    logger.info("=" * 60)
    
    spark = create_spark_session()
    logger.info("Spark Session Iniciada")
    logger.info("Configuracion Geofence: Puerto de La Luz (Las Palmas)")
    
    # Esquema de datos
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

    # Leer de Kafka
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "vessel_positions") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 200) \
        .load()

    # Parsing y Seleccion inicial
    df_parsed = df_kafka.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    # Filtrado de datos invalidos
    df_clean = df_parsed.filter(
        (col("lat").isNotNull()) & 
        (col("lon").isNotNull()) & 
        (col("lat") != 0) & 
        (col("lon") != 0)
    )

    # Transformacion Optimizada (Nativa Spark)
    # Se elimina la UDF de Python y se usan expresiones de columna
    df_processed = df_clean.withColumn(
        "in_port", 
        (col("lat") >= PORT_LAT_MIN) & 
        (col("lat") <= PORT_LAT_MAX) & 
        (col("lon") >= PORT_LON_MIN) & 
        (col("lon") <= PORT_LON_MAX)
    ).withColumn("ingestion_time", current_timestamp())

    # Escribir a Delta Lake
    query = df_processed.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "s3a://lakehouse/checkpoints/vessel_data_v2") \
        .option("path", "s3a://lakehouse/vessel_data_v2") \
        .trigger(processingTime='10 seconds') \
        .start()

    logger.info("=" * 60)
    logger.info("PIPELINE ACTIVO")
    logger.info("Destino: s3a://lakehouse/vessel_data_v2")
    logger.info("Estado: Procesando streams...")
    logger.info("Presiona Ctrl+C para detener el pipeline")
    logger.info("=" * 60)
    
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("")
        logger.info("=" * 60)
        logger.info("SENAL DE INTERRUPCION RECIBIDA")
        logger.info("Deteniendo pipeline de forma segura...")
        query.stop()
        spark.stop()
        logger.info("Pipeline detenido correctamente")
        logger.info("=" * 60)

if __name__ == "__main__":
    main()