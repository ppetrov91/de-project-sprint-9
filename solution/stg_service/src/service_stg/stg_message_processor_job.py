import json
import os
from datetime import datetime, timezone
from logging import Logger
from lib.kafka_client import KafkaConsumer, KafkaProducer
from lib.redis_client import RedisClient
from lib.pg_client import PostgresClient
from typing import Dict


class StgMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 redis: RedisClient,
                 postgres: PostgresClient,
                 batch_size: int,
                 logger: Logger) -> None:
        self.__consumer = consumer
        self.__producer = producer
        self.__redis = redis
        self.__postgres = postgres
        self.__batch_size = batch_size
        self.__logger = logger

    @staticmethod
    def __create_output_message(message: Dict, restaurant: Dict, user: Dict) -> Dict:
        categories_dict = {p["_id"]: p["category"] for p in restaurant["menu"]}
        
        for p in message["payload"]["order_items"]:
            p["category"] = categories_dict[p["id"]]

        return {
            "object_id": message["object_id"],
            "object_type": message["object_type"],
            "sent_dttm": message["sent_dttm"],
            "payload": {
                "id": message["object_id"],
                "date": message["payload"]["date"],
                "cost": message["payload"]["cost"],
                "payment": message["payload"]["payment"],
                "status": message["payload"]["final_status"],
                "restaurant": {
                    "id": restaurant["_id"],
                    "name": restaurant["name"],
                },
                "user": {
                    "id": user["_id"],
                    "name": user["name"],
                    "login": user["login"]
                },
                "products": message["payload"]["order_items"]
            }
        }

    def __get_restaurant_user_from_redis(self, message: Dict):
        restaurant_id = message["payload"]["restaurant"]["id"]
        user_id = message["payload"]["user"]["id"]
        return self.__redis.mget(restaurant_id, user_id)

    def __construct_message(self, mes):
        if mes.error():
            raise Exception(f"An error occured while reading a message from kafka: {mes.error()}")
        
        val = json.loads(mes.value().decode())

        if val.get("object_type", "") != "order":
            return None
        
        restaurant, user = self.__get_restaurant_user_from_redis(val)
        return self.__create_output_message(val, restaurant, user)

    def __save_data_to_pg(self, data):
        self.__logger.info(f" {datetime.now(timezone.utc)}: Start saving data to postgres")
        sql_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 
                                "sql/fill_stg_repository.sql")
    
        with open(sql_path, "r") as f:
            sql = f.read()
            self.__postgres.save_data_to_pg(sql, data)
        
        self.__logger.info(f" {datetime.now(timezone.utc)}: Stop saving data to postgres")

    def __save_data_to_kafka(self, data):
        self.__logger.info(f" {datetime.now(timezone.utc)}: Start saving data to kafka")
        self.__producer.begin_transaction()

        try:
            for mes in data:
                self.__producer.produce(mes)
            self.__producer.commit_transaction()
        except Exception as err:
            self.__logger.info(f" {datetime.now(timezone.utc)}: Stop saving data to kafka")
            self.__producer.abort_transaction()
            raise err
        
        self.__logger.info(f" {datetime.now(timezone.utc)}: Stop saving data to kafka")

    def __process_batch(self) -> None:
        pg_data, kafka_data = [], []
        self.__logger.info(f" {datetime.now(timezone.utc)}: Start getting data from kafka")
        data = self.__consumer.consume(batch_size=self.__batch_size)
        self.__logger.info(f" {datetime.now(timezone.utc)}: Stop getting data from kafka")

        if data is None or len(data) == 0:
            return
        
        for mes in data:
            msg = self.__construct_message(mes)

            if not msg:
                continue

            pg_data.append((msg["object_id"], json.dumps(msg["payload"]), 
                            msg["object_type"], msg["sent_dttm"]))
            
            kafka_data.append(msg)
        
        self.__save_data_to_pg(pg_data)
        self.__save_data_to_kafka(kafka_data)
        self.__consumer.commit()

    def run(self) -> None:
        self.__logger.info(f" {datetime.now(timezone.utc)}: Start processing batch")

        try:
            self.__process_batch()
        except Exception as err:
            self.__logger.error(err, stack_info=True, exc_info=True)

        self.__logger.info(f" {datetime.now(timezone.utc)}: Finish processing batch")