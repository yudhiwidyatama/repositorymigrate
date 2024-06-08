import boto3
import csv
import sys
import logging

#logging.basicConfig(level=logging.DEBUG)

def construct_object_key(digest):
    a = digest[7:9]
    b = digest[7:]
    return f"docker/registry/v2/blobs/sha256/{a}/{b}/data"

endpoint_url = "http://endpoint:9020"
access_key = "ac"
secret_key = "ky"
bucket_name = "bucket1"


endpoint_url2 = "https://endpoint"
access_key2 = "ac2"
secret_key2 = "ky2"
bucket_name2 = "bucket2"
region2 = 'ap-southeast-1'

# Initialize the S3 client for the first service
#s3_client1 = boto3.client('s3', region_name='us-west-2')
s3_client1 = boto3.client("s3", endpoint_url=endpoint_url, aws_access_key_id=access_key, aws_secret_access_key=secret_key)
#s3 = boto3.resource("s3", endpoint_url=endpoint_url, aws_access_key_id=access_key, aws_secret_access_key=secret_key)
# Initialize the S3 client for the second service

s3_client2 = boto3.client("s3", endpoint_url=endpoint_url2, aws_access_key_id=access_key2, aws_secret_access_key=secret_key2, region_name=region2)
#s3_client2 = boto3.client('s3', region_name='us-east-1')
# Read input from standard input (CSV format: namespace,repository,tag,digest)
try:
    reader = csv.reader(sys.stdin)
    print("namespace,repository,tag,differs/identical/not found,digest")
    for row in reader:
        namespace, repository, tag, digest = row
        object_key = construct_object_key(digest)

        # Perform a HEAD request on the first service
        try:
            response1 = s3_client1.head_object(Bucket=bucket_name, Key=object_key)
            size1 = response1['ContentLength']
            etag1 = response1['ETag']
            lm1  = response1['LastModified']
        except s3_client1.exceptions.NoSuchKey:
            size1, etag1, lm1  = None, None, None
            print(f"{namespace}/{repository}:{tag},{digest},{object_key},nosuchkey in 1st bucket ",file=sys.stderr)
        except Exception as e:
            size1, etag1, lm1 = None, None, None
            print(f"{namespace}/{repository}:{tag},{digest},{object_key},1st bucket error {e}",file=sys.stderr)
        # Perform a HEAD request on the second service
        try:
            response2 = s3_client2.head_object(Bucket=bucket_name2, Key=object_key)
            size2 = response2['ContentLength']
            etag2 = response2['ETag']
            lm2 = response2['LastModified']
        except s3_client2.exceptions.NoSuchKey:
            size2, etag2, lm2 = None, None, None
            print("{namespace}/{repository}:{tag},{digest},{object_key},nosuchkey in 2nd bucket ",file=sys.stderr)
        except Exception as e:
            size2, etag2, lm2 = None, None, None
            print(f"{namespace}/{repository}:{tag},{digest},{object_key},2nd bucket error {e}",file=sys.stderr)
        # Compare the object sizes and ETags
        if size1 == size2 and lm1 == lm2:
            status = "identical"
        elif size1 == size2 and lm2 > lm1:
            status = "newer|" + str(lm2) + "vs" + str(lm1)
        elif size1 == size2 and lm2 < lm1:
            status = "older|" + str(lm2) + "vs" + str(lm1)
        elif size1 is None or size2 is None:
            status = "not found"
        else:
            status = "differs|" + str(size1) +"vs"+str(size2)+"|"+lm1+"vs"+lm2
        print(f"{namespace},{repository},{tag},{status},{digest}")
except Exception as e:
    print(f"Error: {e}",file=sys.stderr)
