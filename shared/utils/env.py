import os
from dataclasses import dataclass


@dataclass
class Environment:
    amendments_url = os.getenv("AMENDMENTS_URL")
    cosmos_account_connection_string = os.getenv("COSMOS_ACCOUNT_CONNECTION_STRING")
    cosmos_database = os.getenv("COSMOS_DATABASE")
    storage_account_table_url = os.getenv("STORAGE_ACCOUNT_TABLE_URL")
    storage_account_key = os.getenv("STORAGE_ACCOUNT_KEY")
