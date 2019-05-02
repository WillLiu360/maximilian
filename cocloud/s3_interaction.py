import errno
import os
import boto3
import botocore


class S3Interaction:
    """
    Class to simplify interacting with S3 using boto3
    """
    def __init__(self, aws_access_key, aws_secret_key):
        self.client = boto3.client('s3',
                                   aws_access_key_id=aws_access_key,
                                   aws_secret_access_key=aws_secret_key,
                                   region_name='us-east-1')
        self.s3 = boto3.resource('s3',
                                 aws_access_key_id=aws_access_key,
                                 aws_secret_access_key=aws_secret_key,
                                 region_name='us-east-1')

    def get_bucket(self, bucket_name):
        """Get an s3 bucket obj.

        :param bucket_name: (str) Name of bucket.

        :return boto3.resource.Bucket:
        """
        return self.s3.Bucket(name=bucket_name)

    def save_file_locally(self, bucket, key, local_filename):
        """Save an s3 key a local machine

        :param bucket: (str) Name of bucket.
        :param key: (str) Name of key.
        :param local_filename: (str) Name of file to be saved locally.
        """
        if not os.path.exists(local_filename):
            try:
                os.makedirs(os.path.dirname(local_filename))
            except OSError as exc:
                if exc.errno != errno.EEXIST:
                    raise
            try:
                self.s3.Bucket(bucket).download_file(key, local_filename)
            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] == "404":
                    print("The object does not exist.")
                else:
                    raise

    def get_s3_objects(self, bucket, prefix='', suffix=''):
        """
        Generate objects in an S3 bucket.

        :param bucket: (str) Name of the S3 bucket.
        :param prefix: (str) Only fetch objects whose key starts with
            this prefix (optional).
        :param suffix: (str) Only fetch objects whose keys end with
            this suffix (optional).

        :yield (boto3.resource.Bucket.objectsCollection) Next matching S3 object:
        """
        kwargs = {'Bucket': bucket}

        # If the prefix is a single string (not a tuple of strings), we can
        # do the filtering directly in the S3 API.
        if isinstance(prefix, str):
            kwargs['Prefix'] = prefix

        while True:

            # The S3 API response is a large blob of metadata.
            # 'Contents' contains information about the listed objects.
            resp = self.client.list_objects_v2(**kwargs)

            try:
                contents = resp['Contents']
            except KeyError:
                return

            for obj in contents:
                key = obj['Key']
                if key.startswith(prefix) and key.endswith(suffix):
                    yield obj

            # The S3 API is paginated, returning up to 1000 keys at a time.
            # Pass the continuation token into the next response, until we
            # reach the final page (when this field is missing).
            try:
                kwargs['ContinuationToken'] = resp['NextContinuationToken']
            except KeyError:
                break

    def get_s3_keys(self, bucket, prefix='', suffix=''):
        """
        Generate the keys in an S3 bucket.

        :param bucket: Name of the S3 bucket.
        :param prefix: Only fetch keys that start with this prefix (optional).
        :param suffix: Only fetch keys that end with this suffix (optional).

        :yield (str) Next matching S3 key name:
        """
        for obj in self.get_s3_objects(bucket, prefix, suffix):
            yield obj['Key']

    def put_file_to_s3_from_string(self, bucket, key, string_data):
        """Create key from python string.

        :param bucket: (str) Name of bucket.
        :param key: (str) Name of key.
        :param string_data: (str) Data to be stored in S3.
        """
        self.s3.Object(bucket, key).put(Body=string_data)

    def put_file_to_s3(self, bucket, key, local_filename):
        """Create key from local file.

        :param bucket: (str) Name of bucket.
        :param key: (str) Name of key.
        :param local_filename: (str) Name of file to be uploaded.
        """
        self.s3.meta.client.upload_file(local_filename, bucket, key)

    def put_fileobj_to_s3(self, bucket, key, fileobj):
        """Create key from a byte-array.

        :param bucket: (str) Name of bucket.
        :param key: (str) Name of key.
        :param fileobj: (a file-like object) A file-like object to upload.
        """
        self.client.upload_fileobj(fileobj, bucket, key)

    def delete_key(self, bucket, key):
        """Remove a key from S3.

        :param bucket: (str) Name of bucket.
        :param key: (str) Name of key.
        """
        self.s3.Object(bucket, key).delete()

    def key_exists(self, bucket, key):
        """Check whether key exists in S3 bucket.

        :param bucket:
        :param key:
        :return (bool) True or False:
        """
        bucket = self.s3.Bucket(bucket)
        objects = list(bucket.objects.filter(Prefix=key))
        if len(objects) > 0 and objects[0].key == key:
            return True
        else:
            return False

    def move_key(self, src_bucket, src_key, dst_bucket, dst_key, move=False, overwrite=False):
        """ Move key from one bucket to another.

        :param src_bucket: (str)
        :param src_key: (str)
        :param dst_bucket: (str)
        :param dst_key: (str)
        :param move: (bool) If True, delete source key (optional).
        :param overwrite: (bool) If True, do not overwrite dest key if exists (optional).
        """

        dst_exist = self.key_exists(dst_bucket, dst_key)

        if dst_exist and not overwrite:  # only combo where we don't copy file
            print("did not overwrite " + dst_key + " in bucket " + dst_bucket)
            if move:
                self.delete_key(src_bucket, src_key)

        else:  # lets copy!

            copy_source = {
                'Bucket': src_bucket,
                'Key': src_key
            }

            dst_bucket_obj = self.get_bucket(dst_bucket)

            dst_bucket_obj.copy(copy_source, dst_key)

            if move:
                self.delete_key(src_bucket, src_key)
