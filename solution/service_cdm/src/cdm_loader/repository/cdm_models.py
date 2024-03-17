from typing import List
from uuid import UUID

from pydantic import BaseModel


class CdmModel(BaseModel):
    def table_name(self) -> str:
        pass

    def unique_columns(self) -> List[str]:
        pass

    @classmethod
    def field_names(cls) -> List[str]:
        return cls.__fields__.keys()


class UserProductCounters(CdmModel):
    user_id: UUID
    product_id: UUID
    product_name: str
    order_cnt: int

    def table_name(self) -> str:
        return "cdm.user_product_counters"

    def unique_columns(self) -> List[str]:
        return ["user_id", "product_id"]


class UserCategoryCounters(CdmModel):
    user_id: UUID
    category_id: UUID
    category_name: str
    order_cnt: int

    @property
    def table_name(self) -> str:
        return "cdm.user_category_counters"

    @property
    def unique_columns(self) -> List[str]:
        return ["user_id", "category_id"]
