# Function App - Sentinel - Google Alert Center

This repository hosts an Azure Function App Python code in order to get Google Alert Center logs and send them into Microsoft Sentinel SIEM.

## Pre-requisites

  * A Python 3.11 [Azure Function App](https://docs.microsoft.com/en-us/azure/azure-functions/functions-overview) 
  * A [Log Analytics Workspace](https://docs.microsoft.com/en-us/azure/azure-monitor/logs/log-analytics-overview) with Sentinel enabled on it.
  * A [custom Log Analytics table](https://learn.microsoft.com/en-us/azure/azure-monitor/logs/create-custom-table) to store Google Alert Center logs.
  * A [DCE](https://learn.microsoft.com/en-us/azure/azure-monitor/essentials/data-collection-endpoint-overview?tabs=portal) and [DCR](https://learn.microsoft.com/en-us/azure/azure-monitor/essentials/data-collection-rule-overview) to receive logs from the Function App and send them into Sentinel.
  * A [Storage Account](https://learn.microsoft.com/en-us/azure/storage/common/storage-account-overview)
  * A [Key Vault](https://learn.microsoft.com/en-us/azure/key-vault/general/overview) to store Google secrets.
  * Function [Managed Identity](https://docs.microsoft.com/en-us/azure/active-directory/managed-identities-azure-resources/overview)
    with at least `Storage Blob Data Owner` right on the Storage Account for storing the state file, `Reader` on the 
KeyVault to retrieve secrets.

### Variables

  * **DCE_URL** (required): The ingestion URL of the Data Collection Endpoint. 
  * **DCR_ID** (required): The Data Collection Rule immutable ID.
  * **DCR_STREAM** (required): The Data Collection Rule stream.
  * **KV_URL** (required): The Key Vault URL.
  * **KV_SECRET_GOOGLE_SERVICE_ACCOUNT** (required): The Key Vault Secret name where the Google Service Account information is stored.  
  * **KV_SECRET_GOOGLE_USER** (required): The Key Vault Secret name where the Delegated Google user is stored.
  * **STORAGE_BLOB_FILE** (required): The file used embedding the date used for querying the Google Alert Center API.
  * **STORAGE_CONTAINER** (required): The Storage Account container name where the file is located.
  * **STORAGE_NAME** (required): The Storage Account name.