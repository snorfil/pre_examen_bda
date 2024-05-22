import boto3

bucket_name = 'hotel'
object_key = 'restaurantes.json'

# Configure boto3 to use LocalStack endpoint
s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:4566',
    aws_access_key_id='test',  # use the default access key
    aws_secret_access_key='test',  # use the default secret key
)
response = s3.list_objects_v2(Bucket=bucket_name)
print(response['Contents'])
# Define the bucket name and object key
object_key = response['Contents'][3]['Key']

# Download the file from S3 bucket
response = s3.get_object(Bucket=bucket_name, Key=object_key)
data = response['Body'].read()

print(f"File '{object_key}' downloaded from s3://{bucket_name}/ whose values is {data}")
