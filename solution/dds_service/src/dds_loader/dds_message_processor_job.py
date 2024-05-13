import json
import os
from datetime import datetime, timezone
from logging import Logger
from lib.kafka_client import KafkaConsumer, KafkaProducer
from lib.pg_client import PostgresClient
from typing import Dict


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

    def __construct_message(self, mes: Dict) -> Dict:
        if mes.error():
            raise Exception(f"An error occured while reading a message from kafka: {mes.error()}")

        val = json.loads(mes.value().decode())

        if val.get("object_type", "") != "order":
            return None
        
        return {
            "order_id": val["object_id"],
            "user_id": val["payload"]["user"]["id"],
            "products": [{"product_id": p["id"], 
                          "category_name": p["category"]} for p in val["payload"]["products"]]
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
        self.__logger.info(f" {datetime.now(timezone.utc)}: Start saving data to postgres")
        file_data_dict = self.__get_file_data_dict(load_src, ids, object_type)
        self.__postgres.exec_sql_files(file_data_dict)
        self.__logger.info(f" {datetime.now(timezone.utc)}: Stop saving data to postgres")

    def __process_batch(self) -> None:
        ids, kafka_data = [], []
        self.__logger.info(f" {datetime.now(timezone.utc)}: Start getting data from kafka")
        data = self.__consumer.consume(batch_size=self.__batch_size)
        self.__logger.info(f" {datetime.now(timezone.utc)}: Stop getting data from kafka")

        if data is None or len(data) == 0:
            return
        
        for mes in data:
            msg = self.__construct_message(mes)

            if not msg:
                continue
        
            ids.append(int(msg["order_id"]))
            kafka_data.append(msg)

        self.__save_data_to_pg(ids, "order", "stg-service-orders")
        self.__producer.save_data_to_kafka(kafka_data)
        self.__consumer.commit()


    def run(self) -> None:
        self.__logger.info(f" {datetime.now(timezone.utc)}: Start processing batch")
        
        try:
            self.__process_batch()
        except Exception as err:
            self.__logger.error(err, stack_info=True, exc_info=True)

        self.__logger.info(f" {datetime.now(timezone.utc)}: Finish processing batch")
