from csv import  DictReader, DictWriter
from datetime import timedelta
import sys
import logging
from uuid import uuid4
from minio import Minio
from dotenv import load_dotenv
import os

from boto3 import client, resource
from gen3.auth import Gen3Auth
from gen3.tools.indexing.index_manifest import index_object_manifest

class Gen3MinioClient:
    
    minio_bucket_name = os.getenv("MINIO_BUCKET_NAME")
    minio_api_endpoint = os.getenv("MINIO_ENDPOINT")
    minio_access_key = os.getenv("MINIO_ACCESS_KEY")
    minio_secret_key = os.getenv("MINIO_SECRET_KEY")
    gen3_commons_url = os.getenv("GEN3_COMMONS_URL")
    gen3_credentials = os.getenv("GEN3_CREDENTIALS_PATH")

    client = Minio(
        minio_api_endpoint,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        cert_check=False,
    )

    def __init__(self, minio_access_key, minio_secret_key, minio_bucket_name, minio_endpoint, gen3_commons_url):
        self.minio_access_key = minio_access_key
        self.minio_secret_key = minio_secret_key
        self.minio_bucket_name = minio_bucket_name
        self.minio_endpoint = minio_endpoint
        self.gen3_commons_url = gen3_commons_url
        
    def get_minio_object_names(self):
        objects = client.list_objects(self.minio_bucket_name)
        object_names = []
        for obj in objects:
            object_names.append(obj.object_name)
            print(obj.object_name)
        return object_names
    
    def get_minio_object_names_by_prefix(self, prefix: str):
        objects = client.list_objects(self.minio_bucket_name, prefix=prefix)
        object_names = []
        for obj in objects:
            object_names.append(obj.object_name)
            print(obj.object_name)
        return object_names
    
    # Get presigned URL string to upload file in
    # bucket with response-content-type as application/json
    # and one day expiry.
    def get_minio_presigned_url(self, file_upload_path: str):
        url = client.get_presigned_url(
            "PUT",
            self.minio_bucket_name,
            file_upload_path,
            expires=timedelta(days=1),
            response_headers={"response-content-type": "application/json"},
        )

        return url