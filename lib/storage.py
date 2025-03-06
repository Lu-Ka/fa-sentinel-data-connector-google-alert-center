from azure.core.exceptions import ResourceNotFoundError
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient


class Storage:
    def __init__(self, storage_name, container, blob):
        self.container = container
        self.blob = blob

        credential = DefaultAzureCredential(exclude_shared_token_cache_credential=True)

        self.blob_service_client = BlobServiceClient(
            account_url=f"https://{storage_name}.blob.core.windows.net",
            credential=credential,
        )

        self.blob_client = self.blob_service_client.get_blob_client(
            container=container, blob=blob
        )

    def get(self):
        try:
            return self.blob_client.download_blob().readall().decode()
        except ResourceNotFoundError:
            return None

    def post(self, data):
        try:
            self.blob_client.upload_blob(data, overwrite=True)
        except ResourceNotFoundError:
            self.blob_service_client.create_container(self.container)
            self.blob_client.upload_blob(data, overwrite=True)
