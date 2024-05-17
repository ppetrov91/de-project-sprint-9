import json
import os
from logging import Logger
from lib.kafka_client import KafkaConsumer
from lib.pg_client import PostgresClient


class CDMMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 postgres: PostgresClient,
                 batch_size: int,
                 logger: Logger) -> None:
        self.__consumer = consumer
        self.__postgres = postgres
        self.__batch_size = batch_size
        self.__logger = logger

    @staticmethod
    def __get_file_data_dict(data):
        dirname = os.path.dirname(os.path.abspath(__file__))
        file_data_dict, data_tup = {}, (data, tuple())
        objs = ("user_category_counters", "user_product_counters")

        for obj in objs:
            for query_type in ("fill", "analyze"):
                file_data_dict[f"{dirname}/sql/{query_type}_{obj}.sql"] = data_tup[query_type == "analyze"]

        return file_data_dict

    def __save_data_to_pg(self, ids):
        self.__logger.info("Start saving data to postgres")
        file_data_dict = self.__get_file_data_dict(ids)
        self.__postgres.exec_sql_files(file_data_dict)
        self.__logger.info("Stop saving data to postgres")

    def __process_data(self, data):
        self.__logger.info("Start processing data from kafka")
        ids = []

        for mes in data:
            if mes.error():
                raise Exception(f"An error occured while reading a message from kafka: {mes.error()}")

            val = json.loads(mes.value().decode())

            if val.get("object_type", "") != "order":
                continue
  
            ids.append(val["payload"]["user_id"])

        self.__logger.info("Stop processing data from kafka")
        return ids

    def __process_batch(self):
        self.__logger.info("Start getting data from kafka")
        data = self.__consumer.consume(batch_size=self.__batch_size)
        self.__logger.info("Stop getting data from kafka")

        if data is None or len(data) == 0:
            self.__logger.info("No data was gathered from kafka")
            return

        ids = self.__process_data(data)

        if len(ids) > 0:
            self.__save_data_to_pg(ids)

        self.__consumer.commit()

    def run(self) -> None:
        self.__logger.info("Start processing batch")
        
        try:
            self.__process_batch()
        except Exception as err:
            self.__logger.error(err, stack_info=True, exc_info=True)

        self.__logger.info("Finish processing batch")
