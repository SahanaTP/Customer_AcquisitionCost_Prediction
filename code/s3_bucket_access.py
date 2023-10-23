from enum import unique, IntEnum
import boto3
import os

s3_bucket_name = 'foodmart-dataset'
local_dir = os.getcwd()

@unique
class DirType(IntEnum):
    DIR_TYPE_RAW          = 0
    DIR_TYPE_MERGED       = 1 
    DIR_TYPE_INTERMEDIATE = 2
    DIR_TYPE_TRAIN_TEST   = 3
    DIR_TYPE_RESULT       = 4

dir_type = {
    DirType.DIR_TYPE_RAW.value: 'raw_datasets',
    DirType.DIR_TYPE_MERGED.value: 'merged_data',
    DirType.DIR_TYPE_INTERMEDIATE.value: 'intermediate_data',
    DirType.DIR_TYPE_TRAIN_TEST.value: 'train_test_data',
    DirType.DIR_TYPE_RESULT.value: 'results_data'
}

# Create S3 client
s3 = boto3.client('s3')

def local(type, path=local_dir):
    return os.path.join(path, dir_type[type.value])

def remote(type):
    return dir_type[type.value]+"/"

def check_if_remote_dir_exists(s3_bucket_name, dir_prefix):
    return True if 'Contents' in s3.list_objects(Bucket=s3_bucket_name, Prefix=dir_prefix) else False

def check_if_local_dir_exists(local_directory):
    return True if os.path.exists(local_directory) else False

def fetch_remote_objects(s3_bucket_name, from_path):
    objects = []
    response = s3.list_objects(Bucket=s3_bucket_name, Prefix=from_path)

    for obj in response.get('Contents', []):
        if obj.get('Key') != from_path:
            objects.append(obj.get('Key'))
    return  objects

def fetch_local_objects(from_path):
    objects = []
    files = os.listdir(from_path)

    for file in files:
       if os.path.isfile(os.path.join(from_path, file)):
           objects.append(os.path.join(from_path, file))
    return  objects

def list_object(s3_bucket_name, from_path):
    objects = fetch_remote_objects(s3_bucket_name, from_path)

    if not objects:return

    for obj in objects:
        print(obj)

def download_objects(s3_bucket_name, from_path, to_path, remove_dir=False):
    # Force remove directory if it exists
    if remove_dir and os.path.exists(to_path):
        os.rmdir(to_path)

    # Check if the remote object exists
    if not check_if_remote_dir_exists(s3_bucket_name, from_path):
        return

    # Create new local directory
    if not check_if_local_dir_exists(to_path):
        os.makedirs(to_path)

    objects = fetch_remote_objects(s3_bucket_name, from_path)

    # No objects to download simply return
    if not objects:
        return

    for obj in objects:
        head, tail = os.path.split(obj)
        local_path = os.path.join(to_path, tail)
        remote_path = obj
        s3.download_file(s3_bucket_name, remote_path, local_path)


def upload_objects(s3_bucket_name, from_path, to_path):
    if not os.path.exists(from_path):
        return

    objects = fetch_local_objects(from_path)
    if not objects:
        return

    for obj in objects:
        print("Obj {}".format(obj))
        head, tail = os.path.split(obj)
        local_path = obj
        remote_path = os.path.join(to_path, tail)
        s3.upload_file(local_path, s3_bucket_name, remote_path)


# download_objects(s3_bucket_name, from_path=remote(DirType.DIR_TYPE_RAW), to_path=local(DirType.DIR_TYPE_RAW))
# upload_objects(s3_bucket_name, from_path=local(DirType.DIR_TYPE_RAW), to_path=remote(DirType.DIR_TYPE_INTERMEDIATE))

# list_object(s3_bucket_name, from_path=remote(DirType.DIR_TYPE_RAW))


