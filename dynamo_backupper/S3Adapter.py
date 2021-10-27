import boto3

class S3Adapter:
    def __init__(self, region_name, bucket_name):
        self.region_name = region_name
        self.s3 = boto3.resource('s3', region_name=self.region_name)
        self.bucket = self.s3.Bucket(bucket_name)

    def upload(self, file_stream, key_name):
        self.bucket.upload_fileobj(file_stream, key_name)