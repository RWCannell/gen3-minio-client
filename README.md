## Gen3-Minio Client
### Introduction
A running Gen3 instance is required in order for this application to be purposeful. The `gen3-minio-client` is a Python application that's main purpose is to allow for users to upload data to an on-prem MinIO bucket. The [gen3-client](https://github.com/uc-cdis/cdis-data-client) written in Go uploads data successfully when Gen3 is configured to use an Amazon S3 bucket. However, it fails to upload data to an on-prem MinIO bucket.    

The entire upload process is intended to be as follows:
- create a [blank record](https://uc-cdis.github.io/gen3sdk-python/_build/html/indexing.html#gen3.index.Gen3Index.create_blank) in the `indexd` database   
- generate a presigned URL for the data to be [uploaded](https://uc-cdis.github.io/gen3sdk-python/_build/html/file.html#gen3.file.Gen3File.upload_file)   
- update the blank record with a [GUID](https://uc-cdis.github.io/gen3sdk-python/_build/html/file.html#gen3.file.Gen3File.upload_file_to_guid) by making a PUT request to the `/index​/blank​/{GUID}` endpoint   
- [map](https://uc-cdis.github.io/gen3sdk-python/_build/html/submission.html#gen3.submission.Gen3Submission.submit_record) the uploaded data to the existing graph   

### Environment Variables
Environment variables can be stored inside a `.env` file that is to be stored in the root of this directory. The following variables need to be set:
```env
MINIO_BUCKET_NAME="my-minio-bucket"
MINIO_ENDPOINT="www.miniolocal.co.za"
MINIO_ACCESS_KEY="minioaccesskey"
MINIO_SECRET_KEY="miniosecretkey"
UPLOAD_PATH="/path/to/file"
GEN3_CREDENTIALS_PATH="/gen3-credentials.json"
GEN3_COMMONS_URL="https://www.gen3local.co.za"
```