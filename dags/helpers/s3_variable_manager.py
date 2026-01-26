from airflow.sdk import Variable


class S3VariableManager():
    def __init__(self, s3_bucket_key):
        self.s3_bucket_key = s3_bucket_key

    def get_bucket_name(self):
        # get S3 bucket name
        return Variable.get(self.s3_bucket_key)

    def get_dir_prefix(self, s3_dir_key):
        # get directory name
        return Variable.get(s3_dir_key)
         
