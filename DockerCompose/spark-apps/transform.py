from pyspark.sql import SparkSession
from clickhouse_driver import Client
from pyspark.sql.functions import col, date_format
from pyspark.sql.functions import to_timestamp
import requests
import pandas as pd
from datetime import datetime, timedelta
import numpy as np


spark = SparkSession.builder \
    .appName("YouTubeHistoryTransform") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "admin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()


def convert_element(x):
    if isinstance(x, np.ndarray):
        return str(x.tolist())
    elif isinstance(x, (list, tuple, dict)):
        return str(x)
    elif x is None:
        return ''
    else:
        return str(x)
# Kết nối ClickHouse
client = Client(host='clickhouse', port=9000,user='admin', password='admin123')

# Tạo database và cá dim fact table
client.execute("DROP DATABASE IF EXISTS stage")
client.execute("CREATE DATABASE IF NOT EXISTS stage")

# 2. Tạo bảng dimTime
client.execute("""
CREATE TABLE IF NOT EXISTS stage.dimTime (
    timekey String,
    date Date,
    dayinmonth UInt8,
    dayinyear UInt16,
    month UInt8,
    quarter UInt8,
    year UInt16,
    hour UInt8,
    minute UInt8
) ENGINE = MergeTree()
ORDER BY timekey
""")

# 3. Tạo bảng dimChannel
client.execute("""
CREATE TABLE IF NOT EXISTS stage.dimChannel (
    channel_ID String,
    channel_Name String,
    channel_URL String
) ENGINE = MergeTree()
ORDER BY channel_ID
""")

# 4. Tạo bảng dimVideo
client.execute("""
CREATE TABLE IF NOT EXISTS stage.dimVideo (
    video_ID String,
    video_Title String,
    video_Url String
) ENGINE = MergeTree()
ORDER BY video_ID
""")

# 5. Tạo bảng factHistory
client.execute("""
CREATE TABLE IF NOT EXISTS stage.factHistory (
    id String,
    time_key String,
    video_ID String,
    channel_ID String
) ENGINE = MergeTree()
ORDER BY id
""")

print("Stage database and tables created successfully.")

# 6. Sinh dữ liệu dimTime bằng pandas
start_time = datetime(2024, 4, 1, 0, 0)
end_time = datetime(2025, 12, 31, 23, 59)
time_data = []
current = start_time
while current <= end_time:
    datekey = int(current.strftime("%Y%m%d%H%M"))
    date = current.strftime("%Y-%m-%d")
    day_in_month = current.day
    day_in_year = current.timetuple().tm_yday
    month = current.month
    quarter = (current.month - 1) // 3 + 1
    year = current.year
    hour = current.hour
    minute = current.minute
    time_data.append([
        datekey, date, day_in_month, day_in_year, month, quarter, year, hour, minute
    ])
    current += timedelta(minutes=1)
columns = ["timekey", "date", "dayinmonth", "dayinyear", "month", "quarter", "year", "hour", "minute"]
df_time_dim = pd.DataFrame(time_data, columns=columns)
df_time_dim['timekey'] = df_time_dim['timekey'].astype(str)
df_time_dim['date'] = pd.to_datetime(df_time_dim['date']).dt.date

# 7. Nạp dữ liệu dimTime vào ClickHouse
client.execute('TRUNCATE TABLE IF EXISTS stage.dimTime')
data_time_dim = [tuple(x) for x in df_time_dim.to_numpy()]
client.execute('INSERT INTO stage.dimTime VALUES', data_time_dim)
print("Đã import dimTime vào ClickHouse.")



# 8. Lấy dữ liệu từ S3
silver_df = spark.read.format("delta").load("s3a://silver/youtube/clean_delta")



# 6. Tạo dimChannel
channel_dim = silver_df.select(
    col("channel_name"),
    col("channel_url")
).distinct().toPandas().fillna("").astype(str)
channel_dim.insert(0, 'id', range(1, len(channel_dim) + 1))  # Cột channel_ID tăng dần
channel_dim = channel_dim.rename(columns={'id': 'channel_ID', 'channel_name': 'channel_Name', 'channel_url': 'channel_URL'})

# 7. Tạo dimVideo
video_dim = silver_df.select(
    col("clean_title").alias("video_title"),
    col("titleUrl").alias("video_url")
).distinct().toPandas().fillna("").astype(str)
video_dim.insert(0, 'id', range(1, len(video_dim) + 1))  # Cột video_ID tăng dần
video_dim = video_dim.rename(columns={'id': 'video_ID', 'video_title': 'video_Title', 'video_url': 'video_Url'})

# 8. Mapping Channel và Video ID vào fact
# Tạo dict mapping cho join nhanh
channel_map = dict(zip(zip(channel_dim['channel_Name'], channel_dim['channel_URL']), channel_dim['channel_ID']))
video_map = dict(zip(zip(video_dim['video_Title'], video_dim['video_Url']), video_dim['video_ID']))

# Tạo factHistory
silver_df = silver_df.withColumn(
    "clean_timestamp_minute",
    date_format(to_timestamp(col("clean_timestamp")), "yyyy-MM-dd HH:mm")
)


fact_pd = silver_df.select(
    col("clean_timestamp_minute").alias("time_key"),
    col("clean_title").alias("video_title"),
    col("titleUrl").alias("video_url"),
    col("channel_name"),
    col("channel_url")
).toPandas().fillna("").astype(str)

fact_pd['time_key'] = pd.to_datetime(fact_pd['time_key'], format='%Y-%m-%d %H:%M').dt.strftime('%Y%m%d%H%M')



# Map channel_ID và video_ID
fact_pd['channel_ID'] = fact_pd.apply(lambda row: channel_map.get((row['channel_name'], row['channel_url']), ''), axis=1)
fact_pd['video_ID'] = fact_pd.apply(lambda row: video_map.get((row['video_title'], row['video_url']), ''), axis=1)
fact_pd.insert(0, 'id', range(1, len(fact_pd) + 1))

# Chỉ giữ các cột đúng thứ tự
fact_pd = fact_pd[['id', 'time_key', 'video_ID', 'channel_ID']]


# Chuyển đổi cho channel_dim
for col in channel_dim.columns:
    channel_dim[col] = channel_dim[col].apply(convert_element)

# Chuyển đổi cho video_dim (nếu cần, nhưng thực tế không lỗi)
for col in video_dim.columns:
    video_dim[col] = video_dim[col].apply(convert_element)

# Chuyển đổi cho fact_pd
for col in fact_pd.columns:
    fact_pd[col] = fact_pd[col].apply(convert_element)

data_channel_dim = [tuple(x) for x in channel_dim.to_numpy()]
data_video_dim = [tuple(x) for x in video_dim.to_numpy()]
data_fact_pd = [tuple(x) for x in fact_pd.to_numpy()]

# 9. Nạp vào ClickHouse
client.execute('TRUNCATE TABLE IF EXISTS stage.dimChannel')
client.execute('TRUNCATE TABLE IF EXISTS stage.dimVideo')
client.execute('TRUNCATE TABLE IF EXISTS stage.factHistory')

client.execute('INSERT INTO stage.dimChannel VALUES', data_channel_dim)
client.execute('INSERT INTO stage.dimVideo VALUES', data_video_dim)
client.execute('INSERT INTO stage.factHistory VALUES', data_fact_pd)

print("Đã transform và load dữ liệu vào các bảng dimChannel, dimVideo, factHistory (ID tự tăng) trong ClickHouse.")