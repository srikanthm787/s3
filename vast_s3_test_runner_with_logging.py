
import boto3
import logging
import os
from botocore.config import Config
import json
from botocore import UNSIGNED

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
def generate_zero_data(size_bytes):
    return io.BytesIO(b'\x00' * size_bytes)
    
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
        1 * 1024,                     # 1 KB
        10 * 1024,                    # 10 KB
        100 * 1024,                   # 100 KB
        1 * 1024 * 1024,              # 1 MB
        10 * 1024 * 1024,             # 10 MB
        100 * 1024 * 1024,            # 100 MB
        1 * 1024 * 1024 * 1024        # 1 GB (adjust based on environment)
        # 5 * 1024 * 1024 * 1024      # Optional: 5 GB (can be added if needed)
    ]

    for size in sizes:
        key = f"zeros/object_{size}.bin"

        try:
            # Upload
            log.info(f"Uploading {size} bytes to {key}")
            s3.upload_fileobj(generate_zero_data(size), BUCKET, key)
            log.info(f"Uploaded {key}")

            # Download
            log.info(f"Downloading {key}")
            response = s3.get_object(Bucket=BUCKET, Key=key)
            downloaded_size = len(response['Body'].read())

            if downloaded_size == size:
                log.info(f"Download size matches for {key} ({size} bytes)")
            else:
                log.error(f"Size mismatch: expected {size}, got {downloaded_size} for {key}")

        except Exception as e:
            log.error(f"Error in test_T002 for {key}: {e}")
            

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

def test_T010_key_based_auth():
    log.info("T010 - Key-based Auth")
    try:
        s3.list_buckets()
        log.info("Access successful with provided credentials.")
    except Exception as e:
        log.error(f"Authentication failed: {e}")

def test_T011_deny_specific_prefix_with_restore():
    log.info("T011 - Deny uploads to 'restricted/' prefix (with rollback)")

    # Save current policy (if any)
    try:
        existing_policy = s3.get_bucket_policy(Bucket=BUCKET)
        original_policy = existing_policy["Policy"]
        log.info("Saved original bucket policy")
    except s3.exceptions.from_code("NoSuchBucketPolicy"):
        original_policy = None
        log.info("No existing bucket policy found")

    # Temporary deny policy
    test_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "DenyPutToRestrictedPrefix",
                "Effect": "Deny",
                "Principal": "*",
                "Action": "s3:PutObject",
                "Resource": f"arn:aws:s3:::{BUCKET}/restricted/*"
            }
        ]
    }

    try:
        # Apply test policy
        s3.put_bucket_policy(Bucket=BUCKET, Policy=json.dumps(test_policy))
        log.info("Policy applied: deny uploads to restricted/*")

        # Upload to allowed/
        try:
            s3.put_object(Bucket=BUCKET, Key="allowed/test.txt", Body=b"test")
            log.info("Upload to allowed/ succeeded")
        except Exception as e:
            log.error(f"Upload to allowed/ failed unexpectedly: {e}")

        # Upload to restricted/
        try:
            s3.put_object(Bucket=BUCKET, Key="restricted/test.txt", Body=b"test")
            log.error("Upload to restricted/ succeeded unexpectedly")
        except Exception as e:
            log.info(f"Upload to restricted/ blocked as expected: {e}")

    finally:
        # Restore original policy
        if original_policy:
            s3.put_bucket_policy(Bucket=BUCKET, Policy=original_policy)
            log.info("Restored original bucket policy")
        else:
            s3.delete_bucket_policy(Bucket=BUCKET)
            log.info("Removed test bucket policy")

def test_T012_anonymous_access():
    log.info("T012 - Anonymous Access")
    anon_s3 = boto3.client('s3', endpoint_url=S3_ENDPOINT, config=Config(signature_version=UNSIGNED), verify=VERIFY_SSL)
    try:
        anon_s3.list_objects_v2(Bucket=BUCKET)
        log.error("Anonymous access allowed")
    except Exception as e:
        log.info(f"Anonymous access denied: {e}")

def test_T013_ip_restriction():
    log.info("T013 - IP Restriction")
    my_ip = "203.0.113.1"  # Replace with your IP
    policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Sid": "IPDeny",
            "Effect": "Deny",
            "Principal": "*",
            "Action": "s3:*",
            "Resource": [f"arn:aws:s3:::{BUCKET}", f"arn:aws:s3:::{BUCKET}/*"],
            "Condition": {
                "NotIpAddress": {"aws:SourceIp": my_ip}
            }
        }]
    }
    try:
        s3.put_bucket_policy(Bucket=BUCKET, Policy=json.dumps(policy))
        log.info(f"Bucket policy with IP restriction applied (only allows IP {my_ip})")
    except Exception as e:
        log.error(f"Failed to apply IP restriction policy: {e}")


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
    test_T010_key_based_auth()
    test_T011_bucket_policy()
    test_T012_anonymous_access()
    test_T013_ip_restriction()

