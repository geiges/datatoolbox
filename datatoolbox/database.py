"""
Created on Fri Mar 22 14:55:32 2019

@author: and
"""

import pandas as pd
import os
from dataclasses import dataclass
from typing import Dict, Optional, Mapping, Union
import pyam
import xarray as xr
from pathlib import Path

from . import config
from .sources import Source, GitRepositorySource, DirectorySource
from .data_structures import Datatable, TableSet
from .utilities import plot_query_as_graph
from . import util


def add_field_accessors(cls):
    def make_accessor_property(field):
        return property(lambda s: s.data[field])

    def make_unique_property(field):
        return property(lambda s: s.data[field].unique())

    for field in config.INVENTORY_FIELDS:
        setattr(cls, field, make_accessor_property(field))

        if field != "source":
            setattr(cls, field + "s", make_unique_property(field))

    return cls


@dataclass
@add_field_accessors
class Inventory:
    data: pd.DataFrame
    sources: Dict[str, Source]

    @classmethod
    def from_sources(cls, sources: Mapping[str, Source], **fields) -> "Inventory":
        return cls(data=cls.collect_inventory(sources), sources=dict(sources), **fields)

    @staticmethod
    def collect_inventory(sources: Mapping[str, Source]) -> pd.DataFrame:
        return pd.concat(
            (source.inventory.assign(source=name) for name, source in sources.items()),
            sort=False,
        )

    def info(self, n: int = 80) -> str:
        """Print a summary of the inventory fields

        Parameters
        ----------
        n : int
            The maximum line length

        Written by Daniel Huppmann and published as part of pyam at
        https://github.com/IAMconsortium/pyam/commit/015e8a69f95a20b83dcc302499ad1411114aa539
        """

        fields = config.CONCISE_INVENTORY_FIELDS
        c1 = max(len(i) for i in fields) + 1
        c2 = n - c1 - 5
        info = "Inventory fields:\n" + "\n".join(
            f" * {i:{c1}}: {util.print_list(self.data[i].unique(), c2)}"
            for i in fields
        )

        print(info)

    def source_info(self):
        return pd.DataFrame([s.meta for s in self.sources]).sort_index()

    def filter(
        self, /, regex: bool = False, level: Optional[int] = None, **filters
    ) -> "Inventory":
        data = self.data

        matches = True
        for field, patterns in filters.items():
            matches &= util.pattern_match(data[field], patterns, regex=regex)

        if level is not None:
            matches &= data["variable"].str.count(r"\|", na=True) == level

        # create shallow copies
        return Inventory(pd.DataFrame(data.loc[matches]), dict(self.sources))

    def plot(self, savefig_path=None):
        return plot_query_as_graph(self.data, savefig_path=savefig_path)

    def __str__(self):
        return "=== Datashelf inventory ===\n" + (
            self.data.reset_index(drop=True)[config.CONCISE_INVENTORY_FIELDS].__str__()
        )

    def _repr_html_(self):
        return "=== Datashelf inventory ===<br/>\n" + (
            self.data.reset_index(drop=True)[
                config.CONCISE_INVENTORY_FIELDS
            ]._repr_html_()
        )

    def __iter__(self):
        for key, source in self.data["source"].iteritems():
            yield self.sources[source][key]

    def __contains__(self, key: str) -> bool:
        return key in self.data.index

    def __getitem__(self, key: str) -> Datatable:
        source = self.data.at[key, "source"]
        return self.sources[source][key]

    def to_pyam(self):
        groups = self.data.index.to_series().groupby(by=self.data["source"])
        return pyam.concat(
            self.sources[source].to_pyam(keys) for source, keys in groups
        )

    def to_xarray(self):
        groups = self.data.index.to_series().groupby(by=self.data["source"])
        return xr.concat(
            self.sources[source].to_xarray(keys) for source, keys in groups
        )

    def to_tableset(self):
        groups = self.data.index.to_series().groupby(by=self.data["source"])
        return TableSet.concat(
            self.sources[source].to_tableset(keys) for source, keys in groups
        )


@dataclass
class Database(Inventory):
    """Mutable inventory"""

    @classmethod
    def from_datashelf(cls, path: Union[str, os.PathLike, None] = None) -> "Database":
        if path is None:
            path = config.PATH_TO_DATASHELF
        sources = {}
        for directory in (Path(path) / "database").glob("*"):
            if directory.is_dir:
                sources[directory.name] = (
                    GitRepositorySource.from_path(directory)
                    if (directory / ".git").exists()
                    else DirectorySource.from_path(directory)
                )

        return cls.from_sources(sources)

    def reload_inventory(self, recurse=False) -> None:
        if recurse:
            for source in self.sources.values():
                source.reload_inventory()
        self.data = self.collect_inventory(self.sources)

    def update_sources(self, *sources: Union[str, Source]) -> None:
        if len(sources) == 0:
            sources = list(self.sources.values())

        for source in sources:
            if isinstance(source, str):
                source = self.sources[source]
            source.update()

        self.reload_inventory()

    def add_source(self, source: Source) -> None:
        self.sources[source.name] = source
        self.reload_inventory()

    def remove_source(self, source: Union[str, Source]) -> None:
        if isinstance(source, str):
            source = self.sources[source]

        source.remove()

        del self.sources[source.name]
        self.reload_inventory()
