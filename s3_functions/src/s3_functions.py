import json
import os
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

import boto3
from boto3.exceptions import S3UploadFailedError
from botocore.client import Config
from botocore.exceptions import ClientError

s3_config = Config(max_pool_connections=300, retries=dict(max_attempts=16))

s3_client = boto3.client('s3', config=s3_config)
s3_resource = boto3.resource('s3', config=s3_config)


def get_all_keys(bucket: str = '', prefix: str = '', keys: list = None, marker: str = '', limit: int = -1) -> [str]:
    if keys is None:
        keys = []
    # print(f'get_all_keys() bucket:{bucket}, prefix:{prefix}, keys_len:{len(keys)},marker:{marker}')

    response = s3_client.list_objects(Bucket=bucket, Prefix=prefix, Marker=marker)
    if 'Contents' in response:  # 該当する key がないと response に 'Contents' が含まれない
        keys.extend([content['Key'] for content in response['Contents']])
        if limit > 0 and len(keys) > limit:
            return keys
        if 'IsTruncated' in response:
            return get_all_keys(bucket=bucket, prefix=prefix, keys=keys, marker=keys[-1], limit=limit)
    return keys


def get_all_dirs(bucket: str = '', prefix: str = '', keys: list = None, marker: str = '') -> [str]:
    if keys is None:
        keys = []

    # print(f'get_all_dirs() bucket:{bucket}, prefix:{prefix}, keys_len:{len(keys)},marker:{marker}')

    response = s3_client.list_objects(Bucket=bucket, Prefix=prefix, Delimiter='/', Marker=marker)
    if 'CommonPrefixes' in response:  # 該当する key がないと response に 'Contents' が含まれない
        keys.extend([content['Prefix'] for content in response['CommonPrefixes']])
        if 'IsTruncated' in response:
            return get_all_dirs(bucket=bucket, prefix=prefix, keys=keys, marker=keys[-1])
    return keys


def get_all_keys_and_size(bucket: str = '', prefix: str = '', keys_and_size: list = None, marker: str = '') \
        -> [(str, int)]:
    if keys_and_size is None:
        keys_and_size = []
    # print(f'get_all_keys() bucket:{bucket}, prefix:{prefix}, keys_len:{len(keys)},marker:{marker}')

    response = s3_client.list_objects(Bucket=bucket, Prefix=prefix, Marker=marker)

    if 'Contents' in response:  # 該当する key がないと response に 'Contents' が含まれない
        keys_and_size.extend([(content['Key'], content['Size']) for content in response['Contents']])
        if 'IsTruncated' in response:
            return get_all_keys_and_size(bucket=bucket, prefix=prefix, keys_and_size=keys_and_size,
                                         marker=keys_and_size[-1][0])
    return keys_and_size


def download_s3_file(s3_url: str, dist_filename: str):
    s3_o = urlparse(s3_url)
    s3_bucket = s3_o.netloc
    s3_download_key = s3_o.path.lstrip('/')
    s3_resource.Bucket(s3_bucket).download_file(s3_download_key, dist_filename)
    return


UPLOAD_FILE_MAX_ATTEMPTS = 5

def upload_s3_file(src_filename: str, s3_url: str):
    s3_o = urlparse(s3_url)
    s3_bucket = s3_o.netloc
    s3_upload_key = s3_o.path.lstrip('/')

    attempt = 0

    while True:
        try:
            attempt += 1
            s3_resource.Bucket(s3_bucket).upload_file(src_filename, s3_upload_key,
                                              ExtraArgs={'ACL': 'bucket-owner-full-control'})
        except S3UploadFailedError as e:
            print(f'etl_pipeline_warning upload_s3_file() attempt:{attempt} ClientError:{e}')
            if attempt > UPLOAD_FILE_MAX_ATTEMPTS:
                raise
        else:
            break

    return


def get_s3_json_dict(s3_url: str) -> dict:
    print(f'get_s3_json_dict({s3_url}) start')

    with tempfile.TemporaryDirectory() as dname:
        s3_json_filename = dname + '/' + os.path.basename(s3_url)
        download_s3_file(s3_url, s3_json_filename)
        s3_json_dict = json.load(open(s3_json_filename, 'r'))

    return s3_json_dict


DOWNLOAD_WORKER = 10


def s3_download_multi(src_bucket: str, src_key_list: list, download_dir: str):
    with ThreadPoolExecutor(max_workers=DOWNLOAD_WORKER) as executor:
        future_list = []
        for src_key in src_key_list:
            future_list.append(executor.submit(s3_download_task, src_bucket, src_key, download_dir))
        for future in as_completed(future_list):
            future.result()
    return


def s3_download_task(src_bucket, src_key, download_dir):
    s3_resource.Bucket(src_bucket).download_file(src_key, download_dir + '/' + os.path.basename(src_key))
    return


DELETE_OBJECT_MAX_ATTEMPTS = 5

def delete_s3_file(src_bucket, src_key):
    attempt = 0
    while True:
        try:
            attempt += 1
            s3_client.delete_object(Bucket=src_bucket, Key=src_key)
        except ClientError as e:
            print(f'etl_pipeline_warning delete_s3_file() attempt:{attempt} ClientError:{e}')
            if attempt > DELETE_OBJECT_MAX_ATTEMPTS:
                raise
        else:
            break

    return


def copy_s3_file(src_bucket, src_key, dist_bucket, dist_key):
    s3_client.copy_object(Bucket=dist_bucket, Key=dist_key, CopySource={'Bucket': src_bucket, 'Key': src_key})
    return


def copy_s3_url(src_s3_url, dist_s3_url):
    src_s3_o = urlparse(src_s3_url)
    src_s3_bucket = src_s3_o.netloc
    src_s3_key = src_s3_o.path.lstrip('/')

    dist_s3_o = urlparse(dist_s3_url)
    dist_s3_bucket = dist_s3_o.netloc
    dist_s3_key = dist_s3_o.path.lstrip('/')

    copy_s3_file(src_s3_bucket, src_s3_key, dist_s3_bucket, dist_s3_key)

    return
