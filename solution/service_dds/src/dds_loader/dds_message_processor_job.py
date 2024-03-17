from uuid import UUID
from datetime import datetime
from logging import Logger

from lib.kafka_connect import KafkaConsumer, KafkaProducer
from dds_loader.repository import DdsRepository, DdsOrderBuilder


class DdsMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 dds_repository: DdsRepository,
                 logger: Logger,
                 ) -> None:
        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        self._logger = logger
        self._batch_size = 30

    def _msg_processing(self, msg: dict) -> UUID:
        order = msg["payload"]
        builder = DdsOrderBuilder(order)
        self._dds_repository.insert(builder.h_user)
        for x in builder.h_product:
            self._dds_repository.insert(x)
        for x in builder.h_category:
            self._dds_repository.insert(x)
        self._dds_repository.insert(builder.h_restaurant)
        self._dds_repository.insert(builder.h_order)
        for x in builder.l_order_product:
            self._dds_repository.insert(x)
        for x in builder.l_product_restaurant:
            self._dds_repository.insert(x)
        for x in builder.l_product_category:
            self._dds_repository.insert(x)
        self._dds_repository.insert(builder.l_order_user)
        self._dds_repository.insert(builder.s_user_names)
        for x in builder.s_product_names:
            self._dds_repository.insert(x)
        self._dds_repository.insert(builder.s_restaurant_names)
        self._dds_repository.insert(builder.s_order_cost)
        self._dds_repository.insert(builder.s_order_status)

        return builder.h_user.h_user_pk

    def _create_out_msg(self, h_user_pk: UUID) -> dict:
        lst_products = self._dds_repository.get_data(h_user_pk, ['h_product_pk', 'name'])
        lst_categories = self._dds_repository.get_data(h_user_pk, ['h_category_pk', 'category_name'])

        self._logger.info(f"{datetime.utcnow()}: DDS lst_products: {lst_products}")
        self._logger.info(f"{datetime.utcnow()}: DDS lst_categories: {lst_categories}")

        products = [{'id': str(x[1]), 'name': x[2], 'cnt': x[3]} for x in lst_products]
        categories = [{'id': str(x[1]), 'name': x[2], 'cnt': x[3]} for x in lst_categories]

        return {'user_id': str(h_user_pk), 'products': products, 'categories': categories}

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: DDS START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break

            self._logger.info(f"{datetime.utcnow()}: DDS Input message: {msg}")
            h_user_pk = self._msg_processing(msg)
            self._logger.info(f"{datetime.utcnow()}: DDS Insert to DDS-table-s successful")
            self._logger.info(f"{datetime.utcnow()}: DDS Insert with h_user_pk: {h_user_pk}")
            out_msg = self._create_out_msg(h_user_pk)
            self._logger.info(f"{datetime.utcnow()}: DDS Output message: {out_msg}")
            self._producer.produce(out_msg)
            self._logger.info(f"{datetime.utcnow()}: DDS Send message to topic successful")

        self._logger.info(f"{datetime.utcnow()}: DDS FINISH")
