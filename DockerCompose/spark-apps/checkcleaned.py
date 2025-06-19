from pyspark.sql import SparkSession
from clickhouse_driver import Client
from pyspark.sql.functions import col, date_format
import requests
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

# Tạo database và table
client.execute("CREATE DATABASE IF NOT EXISTS cleaned")

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


# Đọc dữ liệu từ bucket silver và insert vào ClickHouse
silver_df = spark.read.format("delta").load("s3a://silver/youtube/clean_delta")

for c in silver_df.columns:
    silver_df = silver_df.withColumn(c, col(c).cast("string"))

# Sau đó chuyển sang Pandas
pandas_df = silver_df.fillna('').toPandas().astype(str)

data = [tuple(row) for row in pandas_df.values]
client.execute('INSERT INTO cleaned.youtube_clean VALUES', data)

print("Cleaned YouTube history data saved to ClickHouse.")


# Đăng ký database và dataset trong Superset
SUPSERSET_URL = "http://superset:8088"
USERNAME = "admin"
PASSWORD = "admin"

with requests.Session() as session:
    # 1. Đăng nhập lấy access token (và cookie phiên)
    login_resp = session.post(
        f"{SUPSERSET_URL}/api/v1/security/login",
        json={
            "username": USERNAME,
            "password": PASSWORD,
            "provider": "db",
            "refresh": True,
        },
        timeout=10
    )
    login_resp.raise_for_status()
    access_token = login_resp.json()["access_token"]

    # 2. Lấy CSRF token (sử dụng cùng session/cookie)
    headers = {"Authorization": f"Bearer {access_token}"}
    csrf_resp = session.get(
        f"{SUPSERSET_URL}/api/v1/security/csrf_token",
        headers=headers,
        timeout=10
    )
    csrf_resp.raise_for_status()
    csrf_token = csrf_resp.json()["result"]

    # 3. Headers cho các request POST/PUT tiếp theo
    headers_with_csrf = {
        "Authorization": f"Bearer {access_token}",
        "X-CSRFToken": csrf_token,
        "Referer": SUPSERSET_URL,
        "Content-Type": "application/json"
    }

    # 4. Tạo database connection
    database_payload = {
        "database_name": "ClickHouse_cleaned_v2",
        "sqlalchemy_uri": "clickhousedb://admin:admin123@clickhouse:8123/cleaned",
        "engine_parameters": {},
        "parameters": {},
        "expose_in_sqllab": True
    }
    db_resp = session.post(
        f"{SUPSERSET_URL}/api/v1/database/",
        headers=headers_with_csrf,
        json=database_payload,
        timeout=10
    )
    print("STATUS:", db_resp.status_code)
    print("RESPONSE:", db_resp.text)
    db_resp.raise_for_status()
    database_id = db_resp.json()["id"]

    # 5. Tạo dataset
    dataset_payload = {
        "database": database_id,
        "schema": "cleaned",
        "table_name": "youtube_clean"
    }
    dataset_resp = session.post(
        f"{SUPSERSET_URL}/api/v1/dataset/",
        headers=headers_with_csrf,
        json=dataset_payload,
        timeout=10
    )
    print("Dataset response:", dataset_resp.text)
    dataset_resp.raise_for_status()
