from pyspark.sql import SparkSession

# Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("YouTube Ingest to Bronze") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "admin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Đọc data gốc từ local
df = spark.read.option("multiline", True).json("/opt/spark-apps/data/youtube_watch_history_raw.json")

# Ghi xuống MinIO (bucket: bronze) dưới dạng Delta
df.write.format("delta").mode("overwrite").save("s3a://bronze/youtube/watch_history")

print("Ingested YouTube history to bronze bucket in Delta Lake format.")

spark.stop()