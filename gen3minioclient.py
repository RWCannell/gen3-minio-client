import logging
import os
import requests
import sys
import hashlib

from csv import  DictReader, DictWriter
from datetime import timedelta
from uuid import uuid4
from minio import Minio
from dotenv import load_dotenv
from boto3 import client, resource
from gen3.auth import Gen3Auth
from gen3.tools.indexing.index_manifest import index_object_manifest

logging.basicConfig(filename="output.log", level=logging.DEBUG)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

class Gen3MinioClient:
    MANIFEST = "data/manifest/output_manifest_file.tsv"
    COMPLETED = "data/manifest/output_manifest_file.tsv"
    MANIFEST_FIELDS = ['guid', 'file_name', 'md5', 'file_size', 'acl', 'url']

    minio_bucket_name = os.getenv("MINIO_BUCKET_NAME")
    minio_api_endpoint = os.getenv("MINIO_ENDPOINT")
    minio_access_key = os.getenv("MINIO_ACCESS_KEY")
    minio_secret_key = os.getenv("MINIO_SECRET_KEY")
    gen3_commons_url = os.getenv("GEN3_COMMONS_URL")
    gen3_credentials = os.getenv("GEN3_CREDENTIALS_PATH")
    
    client = Minio(
        endpoint=minio_api_endpoint,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        cert_check=False,
    )

    def __init__(self):
        print(f"Initialising Gen3MinioClient with bucket {self.minio_bucket_name} and endpoint https://{self.minio_api_endpoint}...")
        
    def get_minio_objects(self):
        objects = self.client.list_objects(self.minio_bucket_name, recursive=True)
        return objects
    
    def get_minio_objects_by_prefix(self, prefix: str):
        objects = self.client.list_objects(self.minio_bucket_name, prefix=prefix, recursive=True)
        return objects
    
    def get_minio_object_names(self):
        objects = self.client.list_objects(self.minio_bucket_name, recursive=True)
        object_names = []
        for obj in objects:
            object_names.append(obj.object_name)
            print(obj.object_name)
        return object_names
    
    def get_minio_object_names_by_prefix(self, prefix: str):
        objects = self.client.list_objects(self.minio_bucket_name, prefix, recursive=True)
        object_names = []
        for obj in objects:
            object_names.append(obj.object_name)
            print(obj.object_name)
        return object_names
    
    # Get presigned URL string to upload file in
    # bucket with response-content-type as application/json
    # and one day expiry.
    def get_minio_presigned_url(self, file_upload_path: str):
        url = self.client.get_presigned_url(
            "PUT",
            self.minio_bucket_name,
            file_upload_path,
            expires=timedelta(days=1),
            response_headers={"response-content-type": "application/json"},
        )
        return url
    
    def calculate_size_of_file(self, file_path: str):
        data = open(file_path, "rb").read()
        file_size = sys.getsizeof(data)
        print(file_size)
        return file_size
    
    def generate_md5_for_file(self, file_path: str):
        data = open(file_path, "rb").read()
        md5sum = hashlib.md5(data).hexdigest()
        print(md5sum)
        return md5sum
    
    def load_minio_manifest_file(self, manifest_file: str) -> dict:
        with open(manifest_file, "r") as f:
            reader = DictReader(f, delimiter="\t")
            return [row for row in reader]
        
    def create_minio_manifest_file(self, output_manifest_file: str):
        objects = self.client.list_objects(self.minio_bucket_name, recursive=True)
        minio_objects = []
        for obj in objects:
            minio_objects.append({
                "guid": str(uuid4()),
                "file_name": str(obj.object_name),
                "md5": str(obj.etag).strip('"'),
                "file_size": obj.size,
                "acl": "[*]",
                "url": f"https://{self.minio_api_endpoint}/{self.minio_bucket_name}/{obj.object_name}",
            })
        with open(output_manifest_file, "w") as f:
            writer = DictWriter(f, fieldnames=self.MANIFEST_FIELDS, delimiter="\t")
            writer.writeheader()
            for minio_object in minio_objects:
                writer.writerow(minio_object)
        return minio_objects

    def update_minio_manifest_file(self, old_manifest_file: str):
        objects = self.client.list_objects(self.minio_bucket_name, recursive=True)
        updated_minio_objects = []
        existing_minio_objects = self.load_minio_manifest_file(old_manifest_file)
        existing_minio_objects_md5sum_values = [object["md5"] for object in existing_minio_objects]
        for obj in objects:
            if str(obj.etag).strip('"') in existing_minio_objects_md5sum_values:
                continue            
            updated_minio_objects.append({
                "guid": str(uuid4()),
                "file_name": str(obj.object_name),
                "md5": str(obj.etag).strip('"'),
                "file_size": obj.size,
                "acl": "[*]",
                "url": f"https://{self.minio_api_endpoint}/{self.minio_bucket_name}/{obj.object_name}",
            })
        with open(old_manifest_file, "a") as f:
            writer = DictWriter(f, fieldnames=self.MANIFEST_FIELDS, delimiter="\t")
            for minio_object in updated_minio_objects:
                writer.writerow(minio_object)
        
    def create_indexd_manifest(self, manifest_file: str):
        auth = Gen3Auth(refresh_file=self.gen3_credentials)
        self.update_minio_manifest_file(manifest_file)
        indexd_manifest = index_object_manifest(
            commons_url=self.gen3_commons_url,
            manifest_file=manifest_file,
            thread_num=8,
            auth=auth,
            replace_urls=True,
            manifest_file_delimiter="\t", # put "," if the manifest is a CSV file
            submit_additional_metadata_columns=False, # set to True to submit additional metadata to the metadata service
        )

        print(indexd_manifest)
        
if __name__ == '__main__':
    gen3_minio_client = Gen3MinioClient()
    print(gen3_minio_client.create_indexd_manifest("data/manifest/output_manifest_file.tsv"))