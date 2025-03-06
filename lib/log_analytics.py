from azure.core.exceptions import HttpResponseError
from azure.identity import DefaultAzureCredential
from azure.monitor.ingestion import LogsIngestionClient


class LogAnalytics:
    def __init__(self, dce, dcr_id, dcr_stream):
        credential = DefaultAzureCredential(exclude_shared_token_cache_credential=True)

        self.client = LogsIngestionClient(endpoint=dce, credential=credential)

        self.dcr_id = dcr_id
        self.dcr_stream = dcr_stream

    def upload(self, data):
        try:
            self.client.upload(
                rule_id=self.dcr_id, stream_name=self.dcr_stream, logs=data
            )
        except HttpResponseError as e:
            print(f"Upload failed: {e}")
