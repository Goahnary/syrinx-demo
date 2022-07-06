from datetime import datetime
from cryptography.fernet import Fernet
# import base64
# import hashlib
 
# from Crypto import Random
# from Crypto.Cipher import AES

from airflow import models
from airflow.providers.google.cloud.hooks.gcs import upload
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

PROJECT_ID          = "noah-dev-355202" # Change this to your project ID
DAG_ID              = "decrypt_load_gcs_bq"
UPLOAD_FILE_PATH    = "/Users/noahgary/noahgaryio/code/build/syrinx/user_data.csv" #change this to the file location on your PC
FILE_NAME           = "user_data.csv"
BUCKET_NAME         = "user-data-etl-demo"
DATASET_NAME        = "syrinx"
TABLE_NAME          = "users"

# class AESCipher(object):

#     def __init__(self, key): 
#         self.bs = AES.block_size
#         self.key = hashlib.sha256(key.encode()).digest()

#     def encrypt(self, raw):
#         raw = self._pad(raw)
#         iv = Random.new().read(AES.block_size)
#         cipher = AES.new(self.key, AES.MODE_CBC, iv)
#         return base64.b64encode(iv + cipher.encrypt(raw.encode()))

#     def decrypt(self, enc):
#         enc = base64.b64decode(enc)
#         iv = enc[:AES.block_size]
#         print("length of iv is: " + str(len(iv)))
#         cipher = AES.new(self.key, AES.MODE_CBC, iv)
#         return self._unpad(cipher.decrypt(enc[AES.block_size:])).decode('utf-8')

#     def _pad(self, s):
#         return s + (self.bs - len(s) % self.bs) * chr(self.bs - len(s) % self.bs)

#     @staticmethod
#     def _unpad(s):
#         return s[:-ord(s[len(s)-1:])]
        
with models.DAG(
    DAG_ID,
    schedule_interval='@once',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["gcs", "syrinx", "bq"],
) as dag:

    with open('filekey.key', 'rb') as filekey:
        key = filekey.read()

    # cipher = AESCipher(key)
    cipher = Fernet(key)

    with open('encrypted_user_data.csv', 'rb') as enc_file:
        encrypted = enc_file.read()

    decrypted_user_data = cipher.decrypt(encrypted)

    upload_file = upload(
        bucket_name=BUCKET_NAME,
        object_name=FILE_NAME,
        data=decrypted_user_data
    )

    load_csv = GCSToBigQueryOperator(
        task_id='gcs_to_bigquery',
        bucket=BUCKET_NAME,
        source_objects=FILE_NAME,
        destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",
        schema_fields=[
            {'name': 'firstname', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'lastname', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'country', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'username', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'password', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'email', 'type': 'STRING', 'mode': 'NULLABLE'}
        ],
        write_disposition='WRITE_TRUNCATE',
    )

    (
        upload_file
        >> load_csv
    )
