from dataclasses import dataclass
from typing import Any

from dbt.adapters.base.column import Column
from odps.models.table import TableSchema
from odps.types import Decimal, Varchar


@dataclass
class OdpsColumn(Column):
    table_column: TableSchema.TableColumn = None
    comment: str = None

    def literal(self, value) -> str:
        return f"cast({value} as {self.dtype})"

    @property
    def quoted(self) -> str:
        return "`{}`".format(self.column)

    @property
    def data_type(self) -> str:
        return self.dtype

    @classmethod
    def translate_type(cls, dtype: str) -> str:
        return dtype

    @classmethod
    def numeric_type(cls, dtype: str, precision: Any, scale: Any) -> str:
        if precision is None or scale is None:
            return dtype
        else:
            return "{}({},{})".format("decimal", precision, scale)

    def __repr__(self) -> str:
        return "<OdpsColumn {} ({})>".format(self.name, self.data_type)

    def to_column_dict(self, omit_none: bool = True, validate: bool = False) -> dict:
        original_dict = {k: v for k, v in self.__dict__.items() if v is not None}
        # If there are stats, merge them into the root of the dict
        # original_stats = original_dict.pop('table_stats', None)
        # if original_stats:
        #    original_dict.update(original_stats)
        return original_dict

    @classmethod
    def from_odps_column(cls, column: TableSchema.TableColumn):
        char_size = None
        numeric_precision = None
        numeric_scale = None

        if isinstance(column.type, Decimal):
            numeric_precision = column.type.precision
            numeric_scale = column.type.scale
        elif isinstance(column.type, Varchar):
            char_size = column.type.size_limit

        return cls(
            column=column.name,
            dtype=column.type.name,
            char_size=char_size,
            numeric_precision=numeric_precision,
            numeric_scale=numeric_scale,
            table_column=column,
            comment=column.comment,
        )
