from pyspark.sql import SparkSession
from clickhouse_driver import Client
from pyspark.sql.functions import col, date_format

# Tạo SparkSession
spark = SparkSession.builder \
    .appName("YouTubeHistoryCleanCheck") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "admin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()


# Kết nối ClickHouse
client = Client(host='clickhouse', port=9000,user='admin', password='admin123')
# Tao database nếu chưa tồn tại
client.execute("CREATE DATABASE IF NOT EXISTS cleaned")
# Tạo bảng
create_table_query = """
CREATE TABLE IF NOT EXISTS cleaned.youtube_clean (
    timestamp String,
    date String,
    time_of_day String,
    title String,
    video_url String,
    channel_name String,
    channel_url String,
    watch_duration_sec String
) ENGINE = MergeTree()
ORDER BY timestamp
"""
client.execute(create_table_query)

# Đọc dữ liệu từ bucket silver
silver_df = spark.read.format("delta").load("s3a://silver/youtube/clean_delta")

for c in silver_df.columns:
    silver_df = silver_df.withColumn(c, col(c).cast("string"))

# Sau đó chuyển sang Pandas
pandas_df = silver_df.fillna('').toPandas()

# Với Pandas DataFrame (nếu muốn chắc chắn)
pandas_df = pandas_df.astype(str)

client.insert_dataframe('INSERT INTO cleaned.youtube_clean VALUES', pandas_df)
print(pandas_df.head(10))

print("Cleaned YouTube history data saved to ClickHouse.")