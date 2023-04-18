import datetime
import logging
import time
from typing import Mapping, Optional, List, Iterator, MutableMapping

from azure.cosmos import CosmosClient, ContainerProxy, DatabaseProxy
from azure.cosmos.exceptions import CosmosHttpResponseError
from tqdm_loggable.auto import tqdm


class Cosmos:
    _client: CosmosClient
    _database: DatabaseProxy
    _container: ContainerProxy
    _partition_key: str

    def __init__(self, uri: Optional[str] = None, key: Optional[str] = None, connection_string: Optional[str] = None):
        if uri is None or key is None:
            if connection_string is not None:
                uri, key = [c[c.find("=") + 1:] for c in connection_string.split(";") if len(c) > 0]
            else:
                raise ValueError("Either uri or key must be provided")
        self._client = CosmosClient(uri, credential=key)
        logging.info(f"Successfully connected to CosmosDB {uri}")
        logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARN)

    def select_container(self, database: str, container: str, partition_key: str):
        assert len(database) > 0
        assert len(container) > 0
        self._database = self._client.get_database_client(database)
        self._container = self._database.get_container_client(container)
        self._partition_key = partition_key

    def upsert(self, item: MutableMapping):
        self._container.upsert_item(item)

    # TODO : reduce code duplication with Results and a single upsert method
    def upsert_all(self, items: List[MutableMapping], id_key: Optional[str] = None, with_progress_bar: bool = True):
        logging.info(f"Upserting {len(items)} items")
        errors = []
        with tqdm(total=len(items), desc="Upserting elements", unit_scale=True) as bar:
            for i in range(len(items)):
                item = items[i]
                if id_key is not None:
                    item["id"] = item[id_key]
                try:
                    self._container.upsert_item(item)
                except CosmosHttpResponseError:
                    errors.append(item)

                if with_progress_bar:
                    bar.update(1)
                    bar.set_postfix({"time": datetime.datetime.utcnow()})

        if len(errors) > 0:
            logging.warning(f"{len(errors)} requests were in error. Retrying ...")
            time.sleep(1)
            self.upsert_all(errors, id_key)

    def upsert_each(self, items: Iterator[MutableMapping], id_key: Optional[str] = None):
        logging.info("Upserting items ...")
        total = 0
        errors = []
        for item in items:
            if id_key is not None:
                item["id"] = item[id_key]
            try:
                self._container.upsert_item(item)
            except CosmosHttpResponseError:
                errors.append(item)
            total += 1
        logging.info(f"Upserted {total} items. ({len(errors)} errors)")
        if len(errors) > 0:
            logging.warning(f"{len(errors)} requests were in error. Retrying ...")
            time.sleep(1)
            self.upsert_all(errors, id_key)

    def remove(self, item: Mapping):
        self._container.delete_item(item, partition_key=self._partition_key)

    def remove_by_query(self, query: str) -> int:
        total = 0
        for item in self._container.query_items(query=query, enable_cross_partition_query=True):
            self.remove(item)
            total += 1
        return total
