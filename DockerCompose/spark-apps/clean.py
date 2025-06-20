from pyspark.sql import SparkSession
from functools import reduce
from pyspark.sql.functions import col, to_timestamp, to_date, date_format, lag, regexp_replace, unix_timestamp, lead, lit, size ,when, explode_outer
from pyspark.sql.window import Window

# Tạo SparkSession
spark = SparkSession.builder \
    .appName("YouTube History Clean") \
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


# Chuẩn hóa thời gian
flat_df = flat_df.withColumn("timestamp", to_timestamp(col("time")))

flat_df = flat_df.withColumn(
    "clean_timestamp", 
    date_format(col("timestamp"), "yyyy-MM-dd HH:mm:ss")
)


# Xoá tiền tố "Đã xem " ở đầu title
flat_df = flat_df.withColumn(
    "clean_title",
    regexp_replace(col("title"), "^Đã xem ", "")
)

# Sắp xếp theo timestamp tăng dần
windowSpec = Window.orderBy(col("timestamp"))

# Lấy timestamp của video trước
flat_df = flat_df.withColumn("prev_timestamp", lag("timestamp").over(windowSpec))

flat_df = flat_df.withColumn(
    "watching_time",
    (unix_timestamp(col("timestamp")) - unix_timestamp(col("prev_timestamp")))
)

# Chỉ giữ lại các bản ghi có watching_time >= 10 hoặc là None (video đầu tiên)
flat_df = flat_df.filter( col("watching_time") >= 10)

flat_df = flat_df.orderBy(col("timestamp").desc())


# Chọn các cột cần thiết, lấy các trường con từ subtitle struct
flat_df = flat_df.select(
    col("clean_timestamp"),
    col("clean_title"),
    col("titleUrl"),
    col("subtitle.name").alias("channel_name"),
    col("subtitle.url").alias("channel_url"),
    col("watching_time")
)


null_rows = flat_df.filter(
    ~reduce(lambda x, y: x | y, [col(c).isNull() for c in flat_df.columns])
)
null_rows.show(truncate=False)

# Ghi xuống MinIO (bucket: silver) dưới dạng Delta
null_rows.write.format("delta").mode("overwrite").save("s3a://silver/youtube/clean_delta")