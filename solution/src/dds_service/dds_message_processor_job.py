import json
import os
from datetime import datetime, timezone
from logging import Logger
from lib.kafka_client import KafkaConsumer, KafkaProducer
from lib.pg_client import PostgresClient


class DDSMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 postgres: PostgresClient,
                 batch_size: int,
                 logger: Logger) -> None:
        self.__consumer = consumer
        self.__producer = producer
        self.__postgres = postgres
        self.__batch_size = batch_size
        self.__logger = logger

    @staticmethod
    def __get_file_data_dict(ids, object_type, load_src):
        dirname = os.path.dirname(os.path.abspath(__file__))
        bind_vars_data = {
            "load_src": load_src,
            "object_ids": ids,
            "object_type": object_type
        }

        file_data_dict, data_tup = {}, (bind_vars_data, tuple())

        objs = (
            "h_order", "h_user", "h_product", "h_restaurant", "h_category", 
            "l_order_product", "l_order_user", "l_product_category", "l_product_restaurant",
            "s_order_cost", "s_order_status", "s_product_names", "s_restaurant_names",
            "s_user_names"
        )

        for obj in objs:
            for query_type in ("fill", "analyze"):
                file_data_dict[f"{dirname}/sql/{query_type}_{obj}.sql"] = data_tup[query_type == "analyze"]

        return file_data_dict

    def __save_data_to_pg(self, ids, object_type, load_src):
        self.__logger.info("Start saving data to postgres")
        file_data_dict = self.__get_file_data_dict(ids, object_type, load_src)
        self.__postgres.exec_sql_files(file_data_dict)
        self.__logger.info("Stop saving data to postgres")

    def __construct_kafka_data(self, data):
        sent_dttm = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

        return [
            {
                "object_id": int(msg["object_id"]),
                "object_type": "order",
                "sent_dttm": sent_dttm,
                "payload": {
                    "final_status": "CLOSED",
                    "date": msg["payload"]["date"],
                    "user_id": msg["payload"]["user"]["id"],
                    "products": [
                        {"product_id": p["id"], "category_name": p["category"]}
                        for p in msg["payload"]["order_items"]
                    ]
                }
            }
            for msg in data
            if msg["payload"]["final_status"] == "CLOSED"
        ]

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
            return [], []

        ids = [int(msg["object_id"]) for msg in decoded_msgs]
        kafka_data = self.__construct_kafka_data(decoded_msgs)
        self.__logger.info("Stop processing data from kafka")
        return ids, kafka_data

    def __process_batch(self):
        self.__logger.info("Start getting data from kafka")
        data = self.__consumer.consume(batch_size=self.__batch_size)
        self.__logger.info("Stop getting data from kafka")

        if data is None or len(data) == 0:
            self.__logger.info("No data was gathered from kafka")
            return

        ids, kafka_data = self.__process_data(data)

        if len(ids) > 0:
            self.__save_data_to_pg(ids, "order", "stg-service-orders")

        if len(kafka_data) > 0:
            self.__producer.save_data_to_kafka(kafka_data)

        self.__consumer.commit()

    def run(self) -> None:
        self.__logger.info("Start processing batch")
        
        try:
            self.__process_batch()
        except Exception as err:
            self.__logger.error(err, stack_info=True, exc_info=True)

        self.__logger.info("Finish processing batch")
