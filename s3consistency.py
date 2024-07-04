# Author: Sudeesh Varier
# Date: 2024-06-17
# Description: This script is used to test the consistency of an S3 object store from a client perspective on a per-object basis on object creation, deletion, and read-after-write operations.
# The script creates a bucket and performs the following tests:
# 1. read_after_delete: Read an object after it has been deleted
# 2. read_after_create: Read an object after it has been created
# 3. read_after_overwrite: Read an object after it has been overwritten
# 4. list_after_create: List objects after an object has been created
# 5. list_after_delete: List objects after an object has been deleted
# The script takes the following arguments:
# --iterations: Number of iteration per thread per test. Default is 5.
# --threads: Number threads per test. Default is 5.
# --chunk-size: Size in bytes of created files. Default is 1.
# --endpoint: S3 endpoint to use. Default is https://objectstore.basefarm.com.
# --region: S3 region to use. Default is us-east-1.
# --clean: Clean bucket.
# --bucket: Bucket to use for test. Default is s3consistency.
# The script logs the results to /var/log/s3consistency.log.
# This script runs default against the Orange/Basefarm Object Store. To run against a different S3 object store, update the AWS credentials as environment variables and provide the endpoint and region as arguments.

import boto3
import logging
import uuid
import threading
import argparse
import os
import config
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

# Define the AWS credentials as environment variables
# export AWS_ACCESS_KEY_ID='your_access_key_id'
# export AWS_SECRET_ACCESS_KEY='your_secret_access_key'


def initialize_s3_client(endpoint, region):
    session = boto3.Session()
    s3_client = session.client('s3', endpoint_url=endpoint, region_name=region)
    return s3_client

def create_random_file(client, bucket, chunk_size, key=None):
    if key is None:
        key = str(uuid.uuid4())
    body = bytes([0] * chunk_size)
    client.put_object(Bucket=bucket, Key=key, Body=body)
    logger.info(f"PUT object {key}")
    return key

def list_after_delete(client, bucket, iterations, chunk_size):
    count = 0
    total = 0
    for _ in range(iterations):
        key = create_random_file(client, bucket, chunk_size)
        client.delete_object(Bucket=bucket, Key=key)
        logger.info(f"DELETE object {key}")
        response = client.list_objects_v2(Bucket=bucket)
        found = any(obj['Key'] == key for obj in response.get('Contents', []))
        if found:
            count += 1
            logger.info(f"Got a listAfterDelete error, expected {key} file is still listed")
        total += 1
    logger.info(f"listAfterDelete {count}/{total} failed")
    return count

def list_after_create(client, bucket, iterations, chunk_size):
    count = 0
    total = 0
    for _ in range(iterations):
        key = create_random_file(client, bucket, chunk_size)
        response = client.list_objects_v2(Bucket=bucket)
        found = any(obj['Key'] == key for obj in response.get('Contents', []))
        if not found:
            count += 1
            logger.info(f"Got a listAfterCreate error, expected {key} file not listed")
        client.delete_object(Bucket=bucket, Key=key)
        total += 1
    logger.info(f"listAfterCreate {count}/{total} failed")
    return count

def read_after_overwrite(client, bucket, iterations, chunk_size):
    count = 0
    total = 0
    for _ in range(iterations):
        key = create_random_file(client, bucket, chunk_size)
        create_random_file(client, bucket, chunk_size + 1, key)
        response = client.get_object(Bucket=bucket, Key=key)
        body = response['Body'].read()
        if len(body) != chunk_size + 1:
            count += 1
            logger.info(f"Got a readAfterOverwrite error, expected {chunk_size+1} bytes, got {len(body)} instead")
        client.delete_object(Bucket=bucket, Key=key)
        total += 1
    return count

def read_after_delete(client, bucket, iterations, chunk_size):
    count = 0
    total = 0
    for _ in range(iterations):
        key = create_random_file(client, bucket, chunk_size)
        client.delete_object(Bucket=bucket, Key=key)
        try:
            client.get_object(Bucket=bucket, Key=key)
        except client.exceptions.NoSuchKey:
            pass
        else:
            count += 1
        total += 1
    return count

def read_after_create(client, bucket, iterations, chunk_size):
    count = 0
    total = 0
    for _ in range(iterations):
        key = create_random_file(client, bucket, chunk_size)
        try:
            client.get_object(Bucket=bucket, Key=key)
        except client.exceptions.NoSuchKey:
            count += 1
        client.delete_object(Bucket=bucket, Key=key)
        total += 1
    return count

def run_test(client, bucket, fn, iterations, threads, chunk_size):
    errors = []
    def target():
        errors.append(fn(client, bucket, iterations, chunk_size))
    
    threads = [threading.Thread(target=target) for _ in range(threads)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    
    err_count = sum(errors)
    err_pct = (err_count / (iterations * len(threads))) * 100.0
    logger.info(f"{fn.__name__:30} | {iterations * len(threads):10} | {err_count:6} | {err_pct:8.4f}")

    return err_count

def clean_up(client, bucket):
    response = client.list_objects_v2(Bucket=bucket)
    for obj in response.get('Contents', []):
        client.delete_object(Bucket=bucket, Key=obj['Key'])
        logger.info(f"DELETE object {obj['Key']}")

def main():
    parser = argparse.ArgumentParser(description="S3 consistency tests")
    parser.add_argument("--iterations", type=int, default=5, help="Number of iteration per thread per test.")
    parser.add_argument("--threads", type=int, default=5, help="Number threads per test.")
    parser.add_argument("--chunk-size", type=int, default=1, help="Size in bytes of created files")
    parser.add_argument("--endpoint", type=str, default="https://objectstore.basefarm.com", help="S3 endpoint to use")
    parser.add_argument("--region", type=str, default="us-east-1", help="S3 region to use")
    parser.add_argument("--clean", action="store_true", help="Clean bucket")
    parser.add_argument("--bucket", type=str, default="s3consistency", help="Bucket to use for test")
    args = parser.parse_args()

    # Limit the chunk size to 5GB (5 * 1024 * 1024 * 1024 bytes)
    max_chunk_size = 5 * 1024 * 1024 * 1024
    if args.chunk_size > max_chunk_size:
        print(" ** Error ** : chunk-size exceeds the 5GB limit.")
        print("Setting chunk-size to 5GB.")
        args.chunk_size = max_chunk_size

    config.load_s3_credentials()

    client = initialize_s3_client(args.endpoint, args.region)
    bucket_name = args.bucket

    if args.clean:
        clean_up(client, bucket_name)
        return

    test_results = []

    # Run the tests and collect results
    test_results.append(("read_after_delete", run_test(client, bucket_name, read_after_delete, args.iterations, args.threads, args.chunk_size)))
    test_results.append(("read_after_create", run_test(client, bucket_name, read_after_create, args.iterations, args.threads, args.chunk_size)))
    test_results.append(("read_after_overwrite", run_test(client, bucket_name, read_after_overwrite, args.iterations, args.threads, args.chunk_size)))
    test_results.append(("list_after_create", run_test(client, bucket_name, list_after_create, args.iterations, args.threads, args.chunk_size)))
    test_results.append(("list_after_delete", run_test(client, bucket_name, list_after_delete, args.iterations, args.threads, args.chunk_size)))

    # Print the summary of results
    print("\nSummary of Results:")
    print(f"{'Test Name':30} | {'Iterations':10} | {'Errors':6} | {'Error %':8}")
    print("-" * 60)
    for test_name, error_count in test_results:
        total_iterations = args.iterations * args.threads
        error_pct = (error_count / total_iterations) * 100.0
        print(f"{test_name:30} | {total_iterations:10} | {error_count:6} | {error_pct:8.4f}")

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(filename=f"/var/log/s3consistency.log", level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S', format="[%(asctime)s] %(filename)s in %(funcName)s(), line %(lineno)d (%(levelname)s): %(message)s")
    logging.getLogger("boto3").setLevel(logging.WARNING)
    logging.getLogger("threading").setLevel(logging.WARNING)
    logging.getLogger("argparse").setLevel(logging.WARNING)
    logging.getLogger("os").setLevel(logging.WARNING)
    logger = logging.getLogger(__name__)
    logger.info(" <------------------SCRIPT START------------------>")
    main()
    logger.info(" <------------------ SCRIPT END ------------------>")
