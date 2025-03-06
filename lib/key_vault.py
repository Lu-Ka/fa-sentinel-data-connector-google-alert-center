from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient


class KeyVault:
    def __init__(self, kv_url):
        credential = DefaultAzureCredential(exclude_shared_token_cache_credential=True)

        self.secret_client = SecretClient(vault_url=kv_url, credential=credential)

    def get_secret(self, secret):
        try:
            return self.secret_client.get_secret(secret).value
        except Exception as e:
            print(f"{secret} secret failed to be retrieved : {e}")
