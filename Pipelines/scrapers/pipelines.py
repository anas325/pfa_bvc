import io
from datetime import datetime, timezone
from urllib.parse import urlparse

import requests
from minio import Minio


class MinioFilesPipeline:
    def __init__(self, endpoint, access_key, secret_key, bucket):
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.bucket = bucket

    @classmethod
    def from_crawler(cls, crawler):
        s = crawler.settings
        return cls(
            endpoint=s.get("MINIO_ENDPOINT", "localhost:9000"),
            access_key=s.get("MINIO_ACCESS_KEY", "minioadmin"),
            secret_key=s.get("MINIO_SECRET_KEY", "minioadmin"),
            bucket=s.get("MINIO_BUCKET", "scraped-data"),
        )

    def open_spider(self, spider):
        self.client = Minio(
            self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=False,
        )
        if not self.client.bucket_exists(self.bucket):
            self.client.make_bucket(self.bucket)

    def process_item(self, item, spider):
        file_url = item.get("file_url")
        if not file_url:
            return item

        filename = urlparse(file_url).path.rstrip("/").split("/")[-1] or "file"
        date_str = item.get("date", "")
        # Parse year from DD/MM/YYYY or YYYY-MM-DD; fall back to "unknown"
        year = _extract_year(date_str)
        object_name = f"ammc/{year}/{filename}"

        response = requests.get(file_url, timeout=30)
        response.raise_for_status()

        data = response.content
        content_type = response.headers.get("content-type", "application/octet-stream").split(";")[0]

        metadata = {
            "title": item.get("title", ""),
            "source-url": file_url,
            "scraped-at": datetime.now(timezone.utc).isoformat(),
        }
        if date_str:
            metadata["date"] = date_str
        if item.get("category"):
            metadata["category"] = item["category"]

        self.client.put_object(
            self.bucket,
            object_name,
            io.BytesIO(data),
            length=len(data),
            content_type=content_type,
            metadata=metadata,
        )

        item["minio_path"] = f"{self.bucket}/{object_name}"
        del item["file_url"]
        return item


def _extract_year(date_str: str) -> str:
    if not date_str:
        return "unknown"
    # DD/MM/YYYY
    if "/" in date_str and len(date_str) >= 10:
        parts = date_str.split("/")
        if len(parts) == 3 and len(parts[2]) == 4:
            return parts[2]
    # YYYY-MM-DD
    if "-" in date_str and len(date_str) >= 4:
        return date_str[:4]
    return "unknown"
