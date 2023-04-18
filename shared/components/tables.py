from typing import Optional, Dict

from azure.core.credentials import AzureNamedKeyCredential
from azure.data.tables import TableServiceClient, TableClient


class AzureTable:
    __jobs_table_name: str = "statetrackerfunctionsjobruns"

    _service: TableServiceClient
    _table_client: TableClient

    def __init__(self, storage_account_name: str, key: str):
        uri = f"https://{storage_account_name}.table.core.windows.net"
        credential = AzureNamedKeyCredential(storage_account_name, key)
        self._service = TableServiceClient(endpoint=uri, credential=credential)

    def select_table(self, table: str):
        self._table_client = self._service.get_table_client(table_name=table)

    def select_jobs_table(self):
        self.select_table(self.__jobs_table_name)

    def get(self, partition_key: str, row_key: Optional[str] = None, **filters) -> Dict:
        row_key_eq = f"RowKey eq '{row_key}'" if row_key is not None else ""
        filters_query = ' and '.join([f"{k} eq '{v}'" for k, v in filters.items()])
        return self._table_client.query_entities(
            f"PartitionKey eq '{partition_key}' {(' and ' + row_key_eq) if len(row_key_eq) > 0 else ''}{filters_query}")

    def insert(self, entity: Dict, partition_key: Optional[str] = None, row_key: Optional[str] = None) -> Dict:
        entity_cpy = entity.copy()
        if "PartitionKey" not in entity and "RowKey" not in entity:
            if partition_key is None and row_key is None:
                raise ValueError(
                    "Either partition_key or row_key must be provided (inside the entity or with the parameters")
            else:
                entity_cpy["PartitionKey"] = partition_key
                entity_cpy["RowKey"] = row_key

        return self._table_client.create_entity(entity_cpy)
