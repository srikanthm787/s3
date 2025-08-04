
import boto3
import logging
import os
from botocore.config import Config

# ---------- CONFIGURATION ----------
S3_ENDPOINT = "https://object.us1.example.com"
ACCESS_KEY = "YOUR_ACCESS_KEY"
SECRET_KEY = "YOUR_SECRET_KEY"
BUCKET = "test-bucket"
REGION = "us-east-1"
VERIFY_SSL = False  # Set to True if using a valid cert
# -----------------------------------

s3 = boto3.client(
    's3',
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    region_name=REGION,
    config=Config(signature_version='s3v4'),
    verify=VERIFY_SSL
)

def test_T001_bucket_create_delete():
    print("\n T001 - Bucket Create/Delete")
    test_bucket = "boto-test-bucket"
    s3.create_bucket(Bucket=test_bucket)
    log.info(f" Created bucket: {test_bucket}")
    s3.delete_bucket(Bucket=test_bucket)
    log.info(f" Deleted bucket: {test_bucket}")

def test_T002_object_put_get():
    print("\n T002 - Object PUT/GET (1KB to 1GB)")
    sizes = [
        (1 * 1024, "1KB"),
        (1 * 1024 * 1024, "1MB"),
        (100 * 1024 * 1024, "100MB"),
        (1 * 1024 * 1024 * 1024, "1GB")
    ]

    for size, label in sizes:
        key = f"object-{label.lower()}.bin"
        data = os.urandom(size)
        print(f"  Uploading {label} object: {key}")
        s3.put_object(Bucket=BUCKET, Key=key, Body=data)
        log.info(f" Uploaded {label}")

        print(f"  Downloading {label} object: {key}")
        response = s3.get_object(Bucket=BUCKET, Key=key)
        read_bytes = response['Body'].read()
        assert len(read_bytes) == size, f" Size mismatch for {label} object!"
        log.info(f" Verified {label} object roundtrip ({len(read_bytes)} bytes)")

def test_T003_object_list():
    print("\n T003 - Object LIST with pagination and prefix")
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=BUCKET, Prefix="object-", PaginationConfig={"PageSize": 2})
    count = 0
    for page in pages:
        for obj in page.get("Contents", []):
            print(f"- {obj['Key']} ({obj['Size']} bytes)")
            count += 1
    log.info(f" Listed {count} objects")

def test_T004_object_delete():
    print("\n T004 - Object DELETE (batch)")
    keys_to_delete = [{"Key": f"object-{label.lower()}.bin"} for _, label in [
        (1, "1KB"), (1, "1MB"), (1, "100MB"), (1, "1GB")
    ]]
    if keys_to_delete:
        response = s3.delete_objects(Bucket=BUCKET, Delete={"Objects": keys_to_delete})
        deleted = response.get("Deleted", [])
        for obj in deleted:
            log.info(f" Deleted: {obj['Key']}")
    else:
        print(" No objects to delete.")

def test_T005_multipart_upload():
    print("\n T005 - Multipart Upload (>5GB simulated with 6MB)")
    key = "multipart-large.bin"
    file_size = 6 * 1024 * 1024
    part_size = 5 * 1024 * 1024
    data = os.urandom(file_size)

    mpu = s3.create_multipart_upload(Bucket=BUCKET, Key=key)
    upload_id = mpu["UploadId"]
    parts = []

    for i in range(0, file_size, part_size):
        part_num = i // part_size + 1
        chunk = data[i:i+part_size]
        resp = s3.upload_part(
            Bucket=BUCKET,
            Key=key,
            PartNumber=part_num,
            UploadId=upload_id,
            Body=chunk
        )
        parts.append({"PartNumber": part_num, "ETag": resp["ETag"]})

    s3.complete_multipart_upload(
        Bucket=BUCKET,
        Key=key,
        UploadId=upload_id,
        MultipartUpload={"Parts": parts}
    )
    log.info(f" Multipart upload complete: {key}")

def test_T006_versioning():
    print("\n T006 - Versioning")
    # Enable versioning
    s3.put_bucket_versioning(
        Bucket=BUCKET,
        VersioningConfiguration={"Status": "Enabled"}
    )
    log.info(" Versioning enabled")

    # Upload multiple versions
    for i in range(3):
        key = "versioned-object.txt"
        data = f"Version {i+1}".encode()
        s3.put_object(Bucket=BUCKET, Key=key, Body=data)
        log.info(f" Uploaded version {i+1}")

    # List versions
    versions = s3.list_object_versions(Bucket=BUCKET, Prefix="versioned-object.txt")
    for ver in versions.get("Versions", []):
        log.info(f" VersionId: {ver['VersionId']}, Key: {ver['Key']}, IsLatest: {ver['IsLatest']}")


# ---------- LOGGING SETUP ----------
logging.basicConfig(
    filename='vast_s3_test_runner.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
log = logging.getLogger(__name__)
# ------------------------------------

if __name__ == "__main__":
    log.info("Starting VAST S3 test suite")

    test_T001_bucket_create_delete()
    test_T002_object_put_get()
    test_T003_object_list()
    test_T004_object_delete()
    test_T005_multipart_upload()
    test_T006_versioning()
