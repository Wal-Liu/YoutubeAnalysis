from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, to_date, date_format, regexp_replace, unix_timestamp, lead, lit, size ,when, explode_outer
from pyspark.sql.window import Window

# Tạo SparkSession
spark = SparkSession.builder \
    .appName("YouTubeHistoryClean") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "admin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()


raw_df = spark.read.format("delta").load("s3a://bronze/youtube/watch_history")


# Làm phẳng dữ liệu nested JSON: explode_outer để giữ lại bản ghi kể cả khi subtitles rỗng/null
flat_df = raw_df.withColumn("subtitle", explode_outer(col("subtitles")))


# Bước 2: Chuẩn hóa thời gian
# Chuyển 'time' string -> timestamp trước
flat_df = flat_df.withColumn("timestamp", to_timestamp(col("time")))

# Sau đó mới định dạng lại timestamp (loại bỏ milliseconds)
flat_df = flat_df.withColumn(
    "clean_timestamp", 
    date_format(col("timestamp"), "yyyy-MM-dd HH:mm:ss")
)


# Xoá tiền tố "Đã xem " (có dấu cách) ở đầu title
flat_df = flat_df.withColumn(
    "clean_title",
    regexp_replace(col("title"), "^Đã xem ", "")
)


# Chọn các cột cần thiết, lấy các trường con từ subtitle struct
flat_df = flat_df.select(
    col("clean_timestamp"),
    col("clean_title"),
    col("titleUrl"),
    col("subtitle.name").alias("channel_name"),
    col("subtitle.url").alias("channel_url")
)

# In ra 5 dòng đầu để kiểm tra kết quả làm phẳng
flat_df.show(10, truncate=False)