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

    def __construct_message(self, mes):
        if mes.error():
            raise Exception(f"An error occured while reading a message from kafka: {mes.error()}")

        val = json.loads(mes.value().decode())

        if val.get("object_type", "") != "order":
            return None, None
        
        order_id, status = int(val["object_id"]), val["payload"]["status"]

        if status != "CLOSED":
            return order_id, None

        return order_id, {
            "object_id": order_id,
            "object_type": "order",
            "sent_dttm": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            "payload": {
                "status": status,
                "date": val["payload"]["date"],
                "user_id": val["payload"]["user"]["id"],
                "products": [{"product_id": p["id"], 
                              "category_name": p["category"]} for p in val["payload"]["products"]]
            }
        }

    @staticmethod
    def __get_file_data_dict(load_src, ids, object_type):
        dirname = os.path.dirname(os.path.abspath(__file__))

        return {
            f"{dirname}/sql/fill_h_order.sql": [load_src, ids, object_type], 
            f"{dirname}/sql/analyze_h_order.sql": tuple(),
            f"{dirname}/sql/fill_h_user.sql": [load_src, ids, object_type], 
            f"{dirname}/sql/analyze_h_user.sql": tuple(),
            f"{dirname}/sql/fill_h_product.sql": [load_src, ids, object_type], 
            f"{dirname}/sql/analyze_h_product.sql": tuple(),
            f"{dirname}/sql/fill_h_restaurant.sql": [load_src, ids, object_type], 
            f"{dirname}/sql/analyze_h_restaurant.sql": tuple(),
            f"{dirname}/sql/fill_h_category.sql": [load_src, ids, object_type], 
            f"{dirname}/sql/analyze_h_category.sql": tuple(),
            f"{dirname}/sql/fill_l_order_product.sql": [load_src, ids, object_type], 
            f"{dirname}/sql/analyze_l_order_product.sql": tuple(),
            f"{dirname}/sql/fill_l_order_user.sql": [load_src, ids, object_type], 
            f"{dirname}/sql/analyze_l_order_user.sql": tuple(),
            f"{dirname}/sql/fill_l_product_category.sql": [load_src, ids, object_type], 
            f"{dirname}/sql/analyze_l_product_category.sql": tuple(),
            f"{dirname}/sql/fill_l_product_restaurant.sql": [load_src, ids, object_type], 
            f"{dirname}/sql/analyze_l_product_restaurant.sql": tuple(),
            f"{dirname}/sql/fill_s_order_cost.sql": [ids, object_type, load_src],
            f"{dirname}/sql/analyze_s_order_cost.sql": tuple(),
            f"{dirname}/sql/fill_s_order_status.sql": [ids, object_type, load_src],
            f"{dirname}/sql/analyze_s_order_status.sql": tuple(),
            f"{dirname}/sql/fill_s_product_names.sql": [ids, object_type, load_src],
            f"{dirname}/sql/analyze_s_product_names.sql": tuple(),
            f"{dirname}/sql/fill_s_restaurant_names.sql": [ids, object_type, load_src],
            f"{dirname}/sql/analyze_s_restaurant_names.sql": tuple(),
            f"{dirname}/sql/fill_s_user_names.sql": [ids, object_type, load_src],
            f"{dirname}/sql/analyze_s_user_names.sql": tuple(),
        }

    def __save_data_to_pg(self, ids, object_type, load_src):
        self.__logger.info("Start saving data to postgres")
        file_data_dict = self.__get_file_data_dict(load_src, ids, object_type)
        self.__postgres.exec_sql_files(file_data_dict)
        self.__logger.info("Stop saving data to postgres")

    def __process_data(self, data):
        self.__logger.info("Start processing data from kafka")
        ids, kafka_data = [], []

        for mes in data:
            order_id, msg = self.__construct_message(mes)

            if not order_id:
                continue
        
            ids.append(order_id)

            if msg is not None:
                kafka_data.append(msg)

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