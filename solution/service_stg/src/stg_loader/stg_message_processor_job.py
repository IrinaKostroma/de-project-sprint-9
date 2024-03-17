import time
import json
from datetime import datetime
from logging import Logger

from lib.kafka_connect import KafkaConsumer, KafkaProducer
from lib.redis.redis_client import RedisClient
from stg_loader.repository.stg_models import OrderEvents
from stg_loader.repository.stg_repository import StgRepository


class StgMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 redis: RedisClient,
                 stg_repository: StgRepository,
                 batch_size: int,
                 logger: Logger) -> None:
        self._consumer = consumer
        self._producer = producer
        self._redis = redis
        self._stg_repository = stg_repository
        self._batch_size = 100
        self._logger = logger

    def _msg_processing(self, msg: dict) -> str:

        query = self._stg_repository.insert(OrderEvents(object_id=msg['object_id'],
                                                        object_type=msg['object_type'],
                                                        sent_dttm=msg['sent_dttm'],
                                                        payload=json.dumps(msg['payload'])))
        return query

    def _create_out_msg(self, msg: dict) -> dict:
        payload = msg['payload']
        user_id = payload["user"]["id"]
        restaurant_id = payload["restaurant"]["id"]
        order_products = payload["order_items"]

        user = self._redis.get(user_id)
        restaurant = self._redis.get(restaurant_id)
        menu = {}
        [menu.update({p["_id"]: p["category"]}) for p in restaurant["menu"]]
        for p in order_products:
            p["category"] = menu.get(p["id"], "not found")

        out_msg = {
            'object_id': msg['object_id'],
            'object_type': msg['object_type'],
            'payload': {'id': msg['object_id'],  # msg['payload']['id'],
                        'date': payload['date'],
                        'cost': payload['cost'],
                        'payment': payload['payment'],
                        'status': payload['final_status'],
                        'restaurant': {'id': restaurant_id,
                                       'name': restaurant['name']},
                        'user': {'id': user_id,
                                 'name': user['name'],
                                 'login': user['login']},
                        'products': order_products,
                        }
        }
        return out_msg

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: STG START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if msg is None:
                break

            self._logger.info(f"{datetime.utcnow()}: STG Input message: {msg}")

            if "object_type" not in msg:
                self._logger.info(f"{datetime.utcnow()}: STG Bad message, no object_type")
                continue

            query = self._msg_processing(msg)
            self._logger.info(f"{datetime.utcnow()}: STG Insert query: {query}")
            self._logger.info(f"{datetime.utcnow()}: STG Insert to STG-table successful")
            out_msg = self._create_out_msg(msg)
            self._logger.info(f"{datetime.utcnow()}: STG Output message: {out_msg}")
            self._producer.produce(out_msg)
            self._logger.info(f"{datetime.utcnow()}: STG Send message to topic successful")

        self._logger.info(f"{datetime.utcnow()}: STG FINISH")
