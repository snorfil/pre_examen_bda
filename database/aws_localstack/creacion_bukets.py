import boto3
import botocore


def create_s3_buckets():
    # Crear cliente S3
    s3 = boto3.client('s3', endpoint_url='http://localhost:4566')

    buckets = ['empleados-bucket', 'clientes-bucket', 'reservas-bucket', 'menus-bucket', 'platos-bucket']

    for bucket in buckets:
        try:
            s3.create_bucket(Bucket=bucket)
            print(f'Bucket {bucket} creado.')
        except botocore.exceptions.ClientError as e:
            print(f'Error al crear el bucket {bucket}: {e}')


if __name__ == "__main__":
    create_s3_buckets()
