from datetime import datetime
from typing import Any, Dict, List
from uuid import UUID, uuid5

from dds_loader.repository.dds_models import (HubCategory, HubOrder, HubProduct, HubRestaurant, HubUser,
                                              LinkOrderProduct, LinkOrderUser, LinkProductCategory,LinkProductRestaurant,
                                              SatOrderCost, SatOrderStatus, SatProductNames, SatRestaurantNames, SatUserNames)


class DdsOrderBuilder:
    def __init__(self, data: Dict) -> None:
        self._dict = data
        self.source_system = "orders-system-kafka"
        self.order_ns_uuid = UUID("7f288a2e-0ad0-4039-8e59-6c9838d87307")

    def _uuid(self, obj: Any) -> UUID:
        return uuid5(namespace=self.order_ns_uuid, name=str(obj))

    @property
    def h_user(self) -> HubUser:
        user_id = self._dict["user"]["id"]
        return HubUser(
            h_user_pk=self._uuid(user_id),
            user_id=user_id,
            load_dt=datetime.utcnow(),
            load_src=self.source_system,
        )

    @property
    def h_product(self) -> List[HubProduct]:
        products = self._dict["products"]
        h_products = []
        for product in products:
            h_products.append(
                HubProduct(
                    h_product_pk=self._uuid(product["id"]),
                    product_id=product["id"],
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system,
                )
            )

        return h_products

    @property
    def h_category(self) -> List[HubCategory]:
        products = self._dict["products"]
        h_categories = []
        for product in products:
            category_name = product["category"]
            h_categories.append(
                HubCategory(
                    h_category_pk=self._uuid(category_name),
                    category_name=category_name,
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system,
                )
            )

        return h_categories

    @property
    def h_restaurant(self) -> HubRestaurant:
        restaurant_id = self._dict["restaurant"]["id"]
        return HubRestaurant(
            h_restaurant_pk=self._uuid(restaurant_id),
            restaurant_id=restaurant_id,
            load_dt=datetime.utcnow(),
            load_src=self.source_system,
        )

    @property
    def h_order(self) -> HubOrder:
        order_id = self._dict["id"]
        order_dt = self._dict["date"]
        return HubOrder(
            h_order_pk=self._uuid(order_id),
            order_id=order_id,
            order_dt=order_dt,
            load_dt=datetime.utcnow(),
            load_src=self.source_system,
        )

    @property
    def l_order_product(self) -> List[LinkOrderProduct]:
        order_id = self._dict["id"]
        products = self._dict["products"]
        h_order_pk = self._uuid(order_id)
        l_order_products = []
        for product in products:
            h_product_pk = self._uuid(product["id"])
            l_order_products.append(
                LinkOrderProduct(
                    hk_order_product_pk=self._uuid(str(h_order_pk) + str(h_product_pk)),
                    h_order_pk=h_order_pk,
                    h_product_pk=h_product_pk,
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system,
                )
            )

        return l_order_products

    @property
    def l_product_restaurant(self) -> List[LinkProductRestaurant]:
        products = self._dict["products"]
        restaurant_id = self._dict["restaurant"]["id"]
        h_restaurant_pk = self._uuid(restaurant_id)
        l_product_restaurants = []
        for product in products:
            h_product_pk = self._uuid(product["id"])
            l_product_restaurants.append(
                LinkProductRestaurant(
                    hk_product_restaurant_pk=self._uuid(
                        str(h_product_pk) + str(h_restaurant_pk)
                    ),
                    h_product_pk=h_product_pk,
                    h_restaurant_pk=h_restaurant_pk,
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system,
                )
            )

        return l_product_restaurants

    @property
    def l_product_category(self) -> List[LinkProductCategory]:
        products = self._dict["products"]
        l_product_categories = []
        for product in products:
            h_product_pk = self._uuid(product["id"])
            h_category_pk = self._uuid(product["category"])
            hk_product_category_pk = self._uuid(str(h_product_pk) + str(h_category_pk))
            l_product_categories.append(
                LinkProductCategory(
                    hk_product_category_pk=hk_product_category_pk,
                    h_product_pk=h_product_pk,
                    h_category_pk=h_category_pk,
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system,
                )
            )

        return l_product_categories

    @property
    def l_order_user(self) -> LinkOrderUser:
        order_id = self._dict["id"]
        h_order_pk = self._uuid(order_id)
        user_id = self._dict["user"]["id"]
        h_user_pk = self._uuid(user_id)
        return LinkOrderUser(
            hk_order_user_pk=self._uuid(str(h_order_pk) + str(h_user_pk)),
            h_order_pk=h_order_pk,
            h_user_pk=h_user_pk,
            load_dt=datetime.utcnow(),
            load_src=self.source_system,
        )

    @property
    def s_user_names(self) -> SatUserNames:
        user_id = self._dict["user"]["id"]
        h_user_pk = self._uuid(user_id)
        username = self._dict["user"]["name"]
        userlogin = self._dict["user"]["login"]
        return SatUserNames(
            h_user_pk=h_user_pk,
            username=username,
            userlogin=userlogin,
            load_dt=datetime.utcnow(),
            load_src=self.source_system,
            hk_user_names_hashdiff=self._uuid(
                str(user_id) + str(username) + str(userlogin)
            ),
        )

    @property
    def s_product_names(self) -> List[SatProductNames]:
        products = self._dict["products"]
        s_product_names = []
        for product in products:
            product_id = product["id"]
            hk_product_pk = self._uuid(product_id)
            product_name = product["name"]
            s_product_names.append(
                SatProductNames(
                    h_product_pk=hk_product_pk,
                    name=product_name,
                    load_dt=datetime.utcnow(),
                    load_src=self.source_system,
                    hk_product_names_hashdiff=self._uuid(
                        str(hk_product_pk) + str(product_name)
                    ),
                )
            )

        return s_product_names

    @property
    def s_restaurant_names(self) -> SatRestaurantNames:
        restaurant_id = self._dict["restaurant"]["id"]
        h_restaurant_pk = self._uuid(restaurant_id)
        restaurant_name = self._dict["restaurant"]["name"]
        return SatRestaurantNames(
            h_restaurant_pk=h_restaurant_pk,
            name=restaurant_name,
            load_dt=datetime.utcnow(),
            load_src=self.source_system,
            hk_restaurant_names_hashdiff=self._uuid(
                str(h_restaurant_pk) + str(restaurant_name)
            ),
        )

    @property
    def s_order_cost(self) -> SatOrderCost:
        order_id = self._dict["id"]
        h_order_pk = self._uuid(order_id)
        order_cost = self._dict["cost"]
        order_payment = self._dict["payment"]
        return SatOrderCost(
            h_order_pk=h_order_pk,
            cost=order_cost,
            payment=order_payment,
            load_dt=datetime.utcnow(),
            load_src=self.source_system,
            hk_order_cost_hashdiff=self._uuid(
                str(h_order_pk) + str(order_cost) + str(order_payment)
            ),
        )

    @property
    def s_order_status(self) -> SatOrderStatus:
        order_id = self._dict["id"]
        h_order_pk = self._uuid(order_id)
        order_status = self._dict["status"]
        return SatOrderStatus(
            h_order_pk=h_order_pk,
            status=order_status,
            load_dt=datetime.utcnow(),
            load_src=self.source_system,
            hk_order_status_hashdiff=self._uuid(str(h_order_pk) + str(order_status)),
        )
