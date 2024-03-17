from datetime import datetime
from logging import Logger

from lib.kafka_connect import KafkaConsumer, KafkaProducer
from cdm_loader.repository import (CdmRepository,
                                   UserCategoryCounters,
                                   UserProductCounters)


class CdmMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 cdm_repository: CdmRepository,
                 logger: Logger,
                 ) -> None:
        self._consumer = consumer
        self._producer = producer
        self._cdm_repository = cdm_repository
        self._logger = logger
        self._batch_size = 100

    def _msg_processing(self, msg: dict) -> None:
        user_id = msg["user_id"]
        products = [
            UserProductCounters(
                user_id=user_id,
                product_id=obj["id"],
                product_name=obj["name"],
                order_cnt=obj["cnt"],
            )
            for obj in msg["products"]
        ]
        categories = [
            UserCategoryCounters(
                user_id=user_id,
                category_id=obj["id"],
                category_name=obj["name"],
                order_cnt=obj["cnt"],
            )
            for obj in msg["categories"]
        ]
        [self._cdm_repository.insert(product) for product in products]
        [self._cdm_repository.insert(category) for category in categories]

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: CDM START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break

            self._logger.info(f"{datetime.utcnow()}: CDM Input message: {msg}")
            self._msg_processing(msg)
            self._logger.info(f"{datetime.utcnow()}: Insert to CDM successful")

        self._logger.info(f"{datetime.utcnow()}: CDM FINISH")
