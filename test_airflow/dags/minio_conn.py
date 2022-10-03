from minio import Minio
# from minio.error import S3Error
from my_secrets import ACCESS_KEY, SECRET_KEY

class MinioClient:
    def __init__(self):
        self.client = self.get_client()

    def get_client(self):
        client = Minio(
            "sandbox-toys-minio.sandbox.svc.cluster.local:9000",
            access_key=ACCESS_KEY,
            secret_key=SECRET_KEY,
            secure=False
        )
     
        return client

    def add_file(self, name, path):
        # Make 'asiatrip' bucket if not exist.
        found = self.client.bucket_exists("ais")
        if not found:
            self.client.make_bucket("ais")
        else:
            print("Bucket 'ais' already exists")
     
        # Upload '/home/user/Photos/asiaphotos.zip' as object name
        # 'asiaphotos-2015.zip' to bucket 'asiatrip'.
        self.client.fput_object(
            "ais", name, path,
        )
     
        print(
            f"'{path}' is successfully uploaded as "
            f"object '{path}' to bucket 'path'."
        )

