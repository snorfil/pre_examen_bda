import boto3
import os


def upload_files_to_s3():
    s3 = boto3.client('s3', endpoint_url='http://localhost:4566')

    files = {
        'empleados.csv': 'empleados-bucket',
        'clientes.json': 'clientes-bucket',
        'reservas.txt': 'reservas-bucket',
        'menu.csv': 'menus-bucket',
        'platos.csv': 'platos-bucket'
    }

    for file_name, bucket in files.items():
        file_path = os.path.join('/path/to/data', file_name)
        with open(file_path, 'rb') as file_data:
            s3.upload_fileobj(file_data, bucket, file_name)
            print(f'{file_name} subido a {bucket}.')


if __name__ == "__main__":
    upload_files_to_s3()
