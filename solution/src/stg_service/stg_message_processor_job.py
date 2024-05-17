import json
import os
from logging import Logger
from lib.kafka_client import KafkaConsumer, KafkaProducer
from lib.redis_client import RedisClient
from lib.pg_client import PostgresClient


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

    def __get_users_from_redis(self, msgs):
        users = {mes["payload"]["user"]["id"] for mes in msgs}
        users = self.__redis.mget(*users)

        return {
            u["_id"] : {
                "id": u["_id"],
                "name": u["name"],
                "login": u["login"]
            }
            for u in users
        }

    def __get_restaurants_from_redis(self, msgs):
        restaurants = {mes["payload"]["restaurant"]["id"] for mes in msgs}
        restaurants = self.__redis.mget(*restaurants)

        return {
            r["_id"] : {
                "name": r["name"],
                "products": {
                  p["_id"]: p["category" ] for p in r["menu"]
                }
            }
            for r in restaurants
        }

    def __change_kafka_data(self, decoded_msgs):
        users = self.__get_users_from_redis(decoded_msgs)
        restaurants = self.__get_restaurants_from_redis(decoded_msgs)

        for mes in decoded_msgs:
            restaurant_id = mes["payload"]["restaurant"]["id"]
            mes["payload"]["user"] = users[mes["payload"]["user"]["id"]]

            for p in mes["payload"]["order_items"]:
                p["category"] = restaurants[restaurant_id]["products"][p["id"]]

            mes["payload"]["restaurant"] = {
                "id": restaurant_id,
                "name": restaurants[restaurant_id]["name"]
            }

    def __process_data(self, data):
        self.__logger.info("Start processing data from kafka")
        error_msgs = '\n'.join(str(mes.error()) for mes in data if mes.error())

        if error_msgs:
            raise Exception(f"An error occured while reading messages from kafka: {error_msgs}")
        
        decoded_msgs = list(filter(lambda mes: mes.get("object_type", "") == "order", 
                                   map(lambda mes: json.loads(mes.value().decode()), data)
                                  )
                           )

        if len(decoded_msgs) == 0:
            self.__logger.info("No orders were gathered from kafka")
            return []

        self.__change_kafka_data(decoded_msgs)
        self.__logger.info("Stop processing data from kafka")
        return decoded_msgs

    @staticmethod
    def __get_file_data_dict(data):
        dirname = os.path.dirname(os.path.abspath(__file__))
        data = [(m["object_id"], json.dumps(m), m["object_type"], m["sent_dttm"]) for m in data]
        file_data_dict, data_tup = {}, (data, tuple())

        objs = ("order_events",)

        for obj in objs:
            for query_type in ("fill", "analyze"):
                filepath = f"{dirname}/sql/{query_type}_{obj}.sql"
                file_data_dict[filepath] = data_tup[query_type == "analyze"]

        return file_data_dict

    def __save_data_to_pg(self, data):
        self.__logger.info("Start saving data to postgres")
        file_data_dict = self.__get_file_data_dict(data)
        self.__postgres.bulk_data_load(file_data_dict)
        self.__logger.info("Stop saving data to postgres")

    def __process_batch(self):
        self.__logger.info("Start getting data from kafka")
        data = self.__consumer.consume(batch_size=self.__batch_size)
        self.__logger.info("Stop getting data from kafka")

        if data is None or len(data) == 0:
            self.__logger.info("No data was gathered from kafka")
            return
        
        self.__logger.info(f"Gathered {len(data)} messages from kafka")
        data = self.__process_data(data)

        if len(data) > 0:
            self.__save_data_to_pg(data)
            self.__producer.save_data_to_kafka(data)

        self.__consumer.commit()

    def run(self) -> None:
        self.__logger.info("Start processing batch")

        try:
            self.__process_batch()
        except Exception as err:
            self.__logger.error(err, stack_info=True, exc_info=True)

        self.__logger.info("Finish processing batch")