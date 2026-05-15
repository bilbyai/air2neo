from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("air2neo")
except PackageNotFoundError:
    __version__ = "0+unknown"

from .main import Air2Neo, IngestionUpdateType, MetatableConfig

__all__ = ["Air2Neo", "IngestionUpdateType", "MetatableConfig"]
