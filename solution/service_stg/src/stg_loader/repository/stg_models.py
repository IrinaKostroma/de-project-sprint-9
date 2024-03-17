from typing import List
from datetime import datetime

from pydantic import BaseModel


class StgModel(BaseModel):

    def table_name(self) -> str:
        pass

    def unique_columns(self) -> List[str]:
        pass

    @classmethod
    def field_names(cls, **kwargs) -> List[str]:
        return cls.__fields__.keys()


class OrderEvents(StgModel):
    object_id: int
    object_type: str
    sent_dttm: datetime
    payload: str

    @property
    def table_name(self) -> str:
        return "stg.order_events"

    @property
    def unique_columns(self) -> List[str]:
        return ["object_id"]

