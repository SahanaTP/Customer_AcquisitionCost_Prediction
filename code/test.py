from s3_bucket_access import *


list_object(s3_bucket_name, from_path=remote(DirType.DIR_TYPE_RAW))

# download_objects(s3_bucket_name, from_path=remote(DirType.DIR_TYPE_RAW), to_path=local(DirType.DIR_TYPE_RAW))

upload_objects(s3_bucket_name, from_path=local(DirType.DIR_TYPE_MERGED), to_path=remote(DirType.DIR_TYPE_MERGED))
