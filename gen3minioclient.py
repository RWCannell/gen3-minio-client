import logging
import os
import requests
import sys
import hashlib
import json
import re

from csv import  DictReader, DictWriter
from datetime import timedelta
from uuid import uuid4
from minio import Minio
from dotenv import load_dotenv
from boto3 import client, resource
from gen3.auth import Gen3Auth, get_access_token_with_client_credentials, get_access_token_with_key
from gen3.tools.indexing.index_manifest import index_object_manifest
from gen3.index import Gen3Index
from gen3.file import Gen3File
from gen3.submission import Gen3Submission

logging.basicConfig(filename="output.log", level=logging.DEBUG)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

class Gen3MinioClient:
    MANIFEST = "data/manifest/output_manifest_file.tsv"
    COMPLETED = "data/manifest/output_manifest_file.tsv"
    MANIFEST_FIELDS = ['guid', 'file_name', 'md5', 'size', 'acl', 'urls']

    minio_bucket_name = os.getenv("MINIO_BUCKET_NAME")
    minio_api_endpoint = os.getenv("MINIO_ENDPOINT")
    minio_access_key = os.getenv("MINIO_ACCESS_KEY")
    minio_secret_key = os.getenv("MINIO_SECRET_KEY")
    gen3_commons_url = os.getenv("GEN3_COMMONS_URL")
    gen3_credentials = os.getenv("GEN3_CREDENTIALS_PATH")
    gen3_username = os.getenv("GEN3_USERNAME")
    manifest_file_location = os.getenv("MANIFEST_FILE_LOCATION")
    
    client = Minio(
        endpoint=minio_api_endpoint,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        cert_check=False,
    )

    def __init__(self):
        print(f"Initialising Gen3MinioClient with bucket {self.minio_bucket_name} and endpoint https://{self.minio_api_endpoint} for uploader {self.gen3_username}...")
        
    def get_minio_objects(self):
        objects = self.client.list_objects(self.minio_bucket_name, recursive=True)
        minio_objects = []
        for obj in objects:
            object_name = obj.object_name
            guid = str(uuid4())
            index = object_name.rfind('/')
            if (index != -1):
                object_name = object_name[index+1:]
            minio_object = {
                "guid": guid,
                "file_name": object_name,
                "md5": str(obj.etag).strip('"'),
                "size": obj.size,
                "acl": "[*]",
                "urls": [f"https://{self.minio_api_endpoint}/{self.minio_bucket_name}/{obj.object_name}"],
            }
            minio_objects.append(minio_object)
        return minio_objects

    def get_minio_objects_by_prefix(self, prefix: str):
        objects = self.client.list_objects(self.minio_bucket_name, prefix=prefix, recursive=True)
        minio_objects = []
        for obj in objects:
            object_name = obj.object_name
            # did = str(uuid4())
            last_index = object_name.rfind('/')
            if (last_index != -1):
                # did = object_name[ object_name.find("/")+1 : object_name.rfind("/") ]
                object_name = object_name[last_index+1:]
            minio_object = {
                "guid": str(uuid4()),
                "file_name": object_name,
                "md5": str(obj.etag).strip('"'),
                "size": obj.size,
                "acl": "[*]",
                "urls": [f"https://{self.minio_api_endpoint}/{self.minio_bucket_name}/{obj.object_name}"],
            }
            minio_objects.append(minio_object)
        return minio_objects
    
    def get_minio_object_names(self):
        objects = self.client.list_objects(self.minio_bucket_name, recursive=True)
        object_names = []
        for obj in objects:
            object_name = obj.object_name
            index = object_name.rfind('/')
            if (index != -1):
                object_name = object_name[index+1:]
            object_names.append(object_name)
            print(object_name)
        return object_names
    
    def get_minio_object_names_by_prefix(self, prefix: str):
        objects = self.client.list_objects(self.minio_bucket_name, prefix, recursive=True)
        object_names = []
        for obj in objects:
            object_name = obj.object_name
            index = object_name.rfind('/')
            if (index != -1):
                object_name = object_name[index+1:]
            object_names.append(object_name)
            print(object_name)
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
        minio_objects = self.get_minio_objects()
        with open(output_manifest_file, "w") as f:
            writer = DictWriter(f, fieldnames=self.MANIFEST_FIELDS, delimiter="\t")
            writer.writeheader()
            for minio_object in minio_objects:
                writer.writerow(minio_object)
        return minio_objects

    def update_minio_manifest_file(self, old_manifest_file: str):
        minio_objects = self.get_minio_objects()
        updated_minio_objects = []
        existing_minio_objects = self.load_minio_manifest_file(old_manifest_file)
        existing_minio_objects_md5sum_values = [object["md5"] for object in existing_minio_objects]
        for obj in minio_objects:
            if str(obj["md5"]) in existing_minio_objects_md5sum_values:
                continue            
            updated_minio_objects.append({
                "guid": obj["guid"],
                # "did": obj["did"],
                "file_name": obj["file_name"],
                "md5": obj["md5"],
                "size": obj["size"],
                "acl": "[*]",
                "urls": obj["urls"],
                # "uploader": self.gen3_username,
            })
        with open(old_manifest_file, "a") as f:
            writer = DictWriter(f, fieldnames=self.MANIFEST_FIELDS, delimiter="\t")
            for minio_object in updated_minio_objects:
                writer.writerow(minio_object)
                
    def upload_file_to_minio_bucket(self, prefix: str, object_name: str, file_path: str, old_manifest_file: str):
        # Generate prefix and GUID
        did = str(uuid4())
        size_of_file = self.calculate_size_of_file(file_path)
        directory_name_in_bucket = prefix + "/" + did
   
        path = os.path.join(directory_name_in_bucket, object_name)
        
        # Upload data
        result = self.client.fput_object(
            self.minio_bucket_name, path, file_path,
        )
        
        print(
            "Created {0} object; etag: {1}, version-id: {2}".format(
                result.object_name, result.etag, result.version_id,
            ),
        )
        
        minio_object = {
            "guid": str(uuid4()),
            "file_name": object_name,
            "md5": str(result.etag,).strip('"'),
            "size": size_of_file,
            "acl": "[*]",
            "urls": [f"https://{self.minio_api_endpoint}/{self.minio_bucket_name}/{directory_name_in_bucket}/{object_name}"],
        }
        print(f"{minio_object} has been uploaded")
        
        print("Updating manifest with metadata about newly uploaded minio object...")
        self.update_minio_manifest_file(old_manifest_file)
        
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
        
    def submit_sheepdog_record(self, program, project, sheepdog_record):
        auth = Gen3Auth(refresh_file=self.gen3_credentials)
        gen3_submission = Gen3Submission(endpoint=self.gen3_commons_url, auth_provider=auth)
        return gen3_submission.submit_record(program, project, sheepdog_record)
    
    def get_all_records(self):
        auth = Gen3Auth(refresh_file=self.gen3_credentials)
        gen3_index = Gen3Index(auth)
        return gen3_index.get_all_records()
    
    def delete_record_by_guid(self, guid: str):
        auth = Gen3Auth(refresh_file=self.gen3_credentials)
        gen3_index = Gen3Index(auth)
        return gen3_index.delete_record(guid)
        
    def create_blank_record_for_minio_object(self, minio_object):
        auth = Gen3Auth(refresh_file=self.gen3_credentials)
        index = Gen3Index(auth)
        index.create_blank(uploader=minio_object["uploader"], file_name=minio_object["file_name"])
        
    def update_blank_record_for_minio_object(self, minio_object):
        auth = Gen3Auth(refresh_file=self.gen3_credentials)
        index = Gen3Index(auth)
        index.update_blank(
            guid=minio_object["guid"], 
            rev="1", 
            hashes={'md5': minio_object["md5"]}, 
            size=minio_object["size"], 
            urls=minio_object["urls"], 
            authz=None
        )
        
    def create_index_for_minio_object(self):
        minio_objects = self.get_minio_objects()
        for minio_object in minio_objects:
            print("Creating blank record for object '" + minio_object["file_name"] + "'")
            self.create_blank_record_for_minio_object(minio_object)
            
            print("Updating record for object '" + minio_object["file_name"] + "'")
            self.update_blank_record_for_minio_object(minio_object)
        
    
    def update_existing_record(self, guid: str):
        auth = Gen3Auth(refresh_file=self.gen3_credentials)
        gen3_index = Gen3Index(auth)
        gen3_index.update_record(guid)
    
    def get_gen3_presigned_url(self, guid):
        auth = Gen3Auth(refresh_file=self.gen3_credentials)
        gen3_file = Gen3File(endpoint=self.gen3_commons_url, auth_provider=auth)
        gen3_presigned_url = gen3_file.get_presigned_url(guid)
        return gen3_presigned_url
    
    def test_url(self, url):
        x = requests.get(url, verify = False)
        print(x)
        
if __name__ == '__main__':
    gen3_minio_client = Gen3MinioClient()
    # print(gen3_minio_client.create_blank_record_for_minio_object(
    #     {'guid': 'aaff78d3-7440-4f23-849d-c0cdac1523ae', 'did': 'f32d3a0c-da4c-41da-b411-078f879e975e', 'file_name': 'hogwarts_express.jpg', 'md5': '7f5ebf1b42b7f0389ff02bf4106d41a7', 'size': 238202, 'acl': '[*]', 'urls': ['https://cloud05.core.wits.ac.za/gen3-minio-bucket/hogwarts_express.jpg'], 'uploader': 'a0045661@wits.ac.za'}
    # ))
    # gen3_minio_client.create_minio_manifest_file("data/manifest/output_manifest_file.tsv")
    gen3_minio_client.upload_file_to_minio_bucket("PREFIX", "20-Industrial-Rev.pdf", "data/uploads/20-Industrial-Rev.pdf", "data/manifest/output_manifest_file.tsv")
