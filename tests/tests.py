from typing import Optional, Type
import uuid
from snakemake_interface_storage_plugins.tests import TestStorageBase
from snakemake_interface_storage_plugins.storage_provider import StorageProviderBase
from snakemake_interface_storage_plugins.settings import StorageProviderSettingsBase
from snakemake_storage_plugin_ftp import StorageProvider, StorageProviderSettings


class TestStorage(TestStorageBase):
    __test__ = True
    retrieve_only = False  # set to True if the storage is read-only
    store_only = False  # set to True if the storage is write-only
    delete = True  # set to False if the storage does not support deletion

    def get_query(self, tmp_path) -> str:
        # Return a query. If retrieve_only is True, this should be a query that
        # is present in the storage, as it will not be created.
        return "ftp://ftp.dlptest.com/snakemake-test.md"

    def get_query_not_existing(self, tmp_path) -> str:
        folder = uuid.uuid4().hex
        filename = uuid.uuid4().hex
        return f"ftp://{folder}/{filename}.dat"

    def get_storage_provider_cls(self) -> Type[StorageProviderBase]:
        # Return the StorageProvider class of this plugin
        return StorageProvider

    def get_storage_provider_settings(self) -> Optional[StorageProviderSettingsBase]:
        # instantiate StorageProviderSettings of this plugin as appropriate
        # see https://dlptest.com/ftp-test/
        return StorageProviderSettings(
            username="dlpuser", password="rNrKYTX9g7z3RgJRmxWuGHbeu"
        )


class TestStorageFTPS(TestStorage):
    __test__ = True

    def get_query(self, tmp_path) -> str:
        # Return a query. If retrieve_only is True, this should be a query that
        # is present in the storage, as it will not be created.
        return "ftp://ftp.dlptest.com/snakemake-test.md"

    def get_query_not_existing(self, tmp_path) -> str:
        folder = uuid.uuid4().hex
        filename = uuid.uuid4().hex
        return f"ftp://{folder}/{filename}.dat"