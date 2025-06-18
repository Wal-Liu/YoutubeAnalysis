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
    clean_timestamp String,
    clean_title String,
    titleUrl String,
    channel_name String,
    channel_url String,
    watching_time String
) ENGINE = MergeTree()
ORDER BY clean_timestamp
"""
client.execute(create_table_query)

# Đọc dữ liệu từ bucket silver
silver_df = spark.read.format("delta").load("s3a://silver/youtube/clean_delta")

for c in silver_df.columns:
    silver_df = silver_df.withColumn(c, col(c).cast("string"))

# Sau đó chuyển sang Pandas
pandas_df = silver_df.fillna('').toPandas().astype(str)


for col in pandas_df.columns:
    print(f"{col}: {set(type(x) for x in pandas_df[col])}")

print(pandas_df.shape)


data = [tuple(row) for row in pandas_df.values]
client.execute('INSERT INTO cleaned.youtube_clean VALUES', data)


# client.insert_dataframe('INSERT INTO cleaned.youtube_clean VALUES', pandas_df)

print(pandas_df.head(10))
print("Cleaned YouTube history data saved to ClickHouse.")