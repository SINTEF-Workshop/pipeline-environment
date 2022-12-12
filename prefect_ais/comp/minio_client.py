from minio import Minio
from minio.error import S3Error

import sys
sys.path.append("./")

from .my_secrets import ACCESS_KEY, SECRET_KEY

class MinioClient:
    def __init__(self):
        self.client = self.get_client()

    def get_client(self):
        print("Getting client")
        client = Minio(
            "localhost:9900",
            access_key=ACCESS_KEY,
            secret_key=SECRET_KEY,
            secure=False
        )

        return client

    def add_file(self, bucket, bucket_path, path):
        # Make 'asiatrip' bucket if not exist.
        print("Adding file to bucket: " + bucket)


        found = self.client.bucket_exists(bucket)

        print("Found: " + str(found))
        if not found:
            self.client.make_bucket(bucket)
        else:
            print(f"Bucket '{bucket}' already exists")

        # Upload '/home/user/Photos/asiaphotos.zip' as object name
        # 'asiaphotos-2015.zip' to bucket 'asiatrip'.
        self.client.fput_object(
            bucket, bucket_path, path,
        )

        print(
            f"'{path}' is successfully uploaded as "
            f"object '{path}' to bucket 'path'."
        )

