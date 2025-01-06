from enum import Enum


class ProductType(str, Enum):
    DATA_STUDIO = "DataStudio"
    DATA_QUALITY = "DataQuality"
    DATA_SERVICE = "DataService"
    DATA_CATALOG = "DataCatalog"
    DATA_SOURCE = "DataSource"
