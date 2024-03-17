from datetime import datetime
from typing import List
from uuid import UUID

from pydantic import BaseModel


class DdsModel(BaseModel):
    def table_name(self) -> str:
        pass

    def unique_columns(self) -> List[str]:
        pass

    @classmethod
    def field_names(cls) -> List[str]:
        return cls.__fields__.keys()


class HubUser(DdsModel):
    h_user_pk: UUID
    user_id: str
    load_dt: datetime
    load_src: str

    @property
    def table_name(self) -> str:
        return "dds.h_user"

    @property
    def unique_columns(self) -> List[str]:
        return ["h_user_pk"]


class HubProduct(DdsModel):
    h_product_pk: UUID
    product_id: str
    load_dt: datetime
    load_src: str

    @property
    def table_name(self) -> str:
        return "dds.h_product"

    @property
    def unique_columns(self) -> List[str]:
        return ["h_product_pk"]


class HubCategory(DdsModel):
    h_category_pk: UUID
    category_name: str
    load_dt: datetime
    load_src: str

    @property
    def table_name(self) -> str:
        return "dds.h_category"

    @property
    def unique_columns(self) -> List[str]:
        return ["h_category_pk"]


class HubRestaurant(DdsModel):
    h_restaurant_pk: UUID
    restaurant_id: str
    load_dt: datetime
    load_src: str

    @property
    def table_name(self) -> str:
        return "dds.h_restaurant"

    @property
    def unique_columns(self) -> List[str]:
        return ["h_restaurant_pk"]


class HubOrder(DdsModel):
    h_order_pk: UUID
    order_id: int
    order_dt: datetime
    load_dt: datetime
    load_src: str

    @property
    def table_name(self) -> str:
        return "dds.h_order"

    @property
    def unique_columns(self) -> List[str]:
        return ["h_order_pk"]


class LinkOrderProduct(DdsModel):
    hk_order_product_pk: UUID
    h_order_pk: UUID
    h_product_pk: UUID
    load_dt: datetime
    load_src: str

    @property
    def table_name(self) -> str:
        return "dds.l_order_product"

    @property
    def unique_columns(self) -> List[str]:
        return ["hk_order_product_pk"]


class LinkProductRestaurant(DdsModel):
    hk_product_restaurant_pk: UUID
    h_product_pk: UUID
    h_restaurant_pk: UUID
    load_dt: datetime
    load_src: str

    @property
    def table_name(self) -> str:
        return "dds.l_product_restaurant"

    @property
    def unique_columns(self) -> List[str]:
        return ["hk_product_restaurant_pk"]


class LinkProductCategory(DdsModel):
    hk_product_category_pk: UUID
    h_product_pk: UUID
    h_category_pk: UUID
    load_dt: datetime
    load_src: str

    @property
    def table_name(self) -> str:
        return "dds.l_product_category"

    @property
    def unique_columns(self) -> List[str]:
        return ["hk_product_category_pk"]


class LinkOrderUser(DdsModel):
    hk_order_user_pk: UUID
    h_order_pk: UUID
    h_user_pk: UUID
    load_dt: datetime
    load_src: str

    @property
    def table_name(self) -> str:
        return "dds.l_order_user"

    @property
    def unique_columns(self) -> List[str]:
        return ["hk_order_user_pk"]


class SatUserNames(DdsModel):
    h_user_pk: UUID
    username: str
    userlogin: str
    load_dt: datetime
    load_src: str
    hk_user_names_hashdiff: UUID

    @property
    def table_name(self) -> str:
        return "dds.s_user_names"

    @property
    def unique_columns(self) -> List[str]:
        return ["hk_user_names_hashdiff"]


class SatProductNames(DdsModel):
    h_product_pk: UUID
    name: str
    load_dt: datetime
    load_src: str
    hk_product_names_hashdiff: UUID

    @property
    def table_name(self) -> str:
        return "dds.s_product_names"

    @property
    def unique_columns(self) -> List[str]:
        return ["hk_product_names_hashdiff"]


class SatRestaurantNames(DdsModel):
    h_restaurant_pk: UUID
    name: str
    load_dt: datetime
    load_src: str
    hk_restaurant_names_hashdiff: UUID

    @property
    def table_name(self) -> str:
        return "dds.s_restaurant_names"

    @property
    def unique_columns(self) -> List[str]:
        return ["hk_restaurant_names_hashdiff"]


class SatOrderCost(DdsModel):
    h_order_pk: UUID
    cost: float
    payment: float
    load_dt: datetime
    load_src: str
    hk_order_cost_hashdiff: UUID

    @property
    def table_name(self) -> str:
        return "dds.s_order_cost"

    @property
    def unique_columns(self) -> List[str]:
        return ["hk_order_cost_hashdiff"]


class SatOrderStatus(DdsModel):
    h_order_pk: UUID
    status: str
    load_dt: datetime
    load_src: str
    hk_order_status_hashdiff: UUID

    @property
    def table_name(self) -> str:
        return "dds.s_order_status"

    @property
    def unique_columns(self) -> List[str]:
        return ["hk_order_status_hashdiff"]
