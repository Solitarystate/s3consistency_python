import os

# S3 Configuration
AWS_ACCESS_KEY_ID = 'your_access_key_here'
AWS_SECRET_ACCESS_KEY = 'your_secret_key_here'
S3_BUCKET_NAME = 'your_bucket_name_here'

def load_s3_credentials():
    """
    Load s3 credentials into the environment variables.
    """
    os.environ['AWS_ACCESS_KEY_ID'] = AWS_ACCESS_KEY_ID
    os.environ['AWS_SECRET_ACCESS_KEY'] = AWS_SECRET_ACCESS_KEY
    os.environ['S3_BUCKET_NAME'] = S3_BUCKET_NAME
    print("S3 credentials and S3 bucket name loaded into environment variables.")