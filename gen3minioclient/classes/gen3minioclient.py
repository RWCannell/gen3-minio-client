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
    MANIFEST = "./manifest.tsv"
    COMPLETED = "./completed.tsv"
    MANIFEST_FIELDS = ['GUID', 'md5', 'size', 'acl', 'url']

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

    # def __init__(self, minio_access_key, minio_secret_key, minio_bucket_name, minio_endpoint, gen3_commons_url):
    #     self.minio_access_key = minio_access_key
    #     self.minio_secret_key = minio_secret_key
    #     self.minio_bucket_name = minio_bucket_name
    #     self.minio_endpoint = minio_endpoint
    #     self.gen3_commons_url = gen3_commons_url
        
    def __init__(self):
        print(f"Initialising Gen3MinioClient with bucket {self.minio_bucket_name} and endpoint https://{self.minio_api_endpoint}")
        
    def get_minio_objects(self):
        objects = self.client.list_objects(self.minio_bucket_name)
        return objects
    
    def get_minio_objects_by_prefix(self, prefix: str):
        objects = self.client.list_objects(self.minio_bucket_name, prefix=prefix)
        return objects
    
    def get_minio_object_names(self):
        objects = self.client.list_objects(self.minio_bucket_name)
        object_names = []
        for obj in objects:
            object_names.append(obj.object_name)
            print(obj.object_name)
        return object_names
    
    def get_minio_object_names_by_prefix(self, prefix: str):
        objects = self.client.list_objects(self.minio_bucket_name, prefix=prefix)
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
    
    def get_size_of_object(file_path: str):
        data = open(file_path, "rb").read()
        file_size = sys.getsizeof(data)
        print(file_size)
        return file_size
    
    def calculate_size_of_file(file_path: str):
        data = open(file_path, "rb").read()
        file_size = sys.getsizeof(data)
        print(file_size)
        return file_size
    
    def generate_md5_for_file(file_path: str):
        data = open(file_path, "rb").read()
        md5sum = hashlib.md5(data).hexdigest()
        print(md5sum)
        return md5sum
    
    def load_minio_manifest_file(filename: str) -> dict:
        with open(filename, "r") as f:
            reader = DictReader(f, delimiter="\t")
            return {row["GUID"]: row for row in reader}
    
    def construct_minio_manifest_file(self, filename):
        objects = self.client.list_objects(self.minio_bucket_name)
        
        with open(filename, "w") as f:
            writer = DictWriter(f, fieldnames=self.MANIFEST_FIELDS, delimiter="\t")
            writer.writeheader()
            for key, minio_object in objects.items():
                writer.writerow(minio_object)

    def create_minio_manifest_file(self):
        auth = Gen3Auth(refresh_file=self.gen3_credentials)

        already_uploaded = self.load_minio_manifest_file(self.COMPLETED)
        print(already_uploaded)

        minio_objects = self.get_minio_objects_by_prefix(prefix="PREFIX")
        new_manifest_dict = {}
        for key, minio_object in minio_objects.items():
            if key in already_uploaded:
                continue
            new_manifest_dict[key] = {
                "GUID": str(uuid4()),
                "md5": str(minio_object.e_tag).strip('"'),
                "size": minio_object.size,
                "acl": "[*]",
                "url": f"https://{self.minio_api_endpoint}/{key}"
            }
        self.construct_minio_manifest_file(self.MANIFEST, new_manifest_dict)

        indexd_manifest = index_object_manifest(
            commons_url=self.gen3_commons_url,
            manifest_file=self.MANIFEST,
            thread_num=8,
            auth=auth,
            replace_urls=True,
            manifest_file_delimiter="\t", # put "," if the manifest is csv file
            submit_additional_metadata_columns=False, # set to True to submit additional metadata to the metadata service
        )

        print(indexd_manifest)
        
if __name__ == '__main__':
    gen3_minio_client = Gen3MinioClient()
    print(gen3_minio_client)