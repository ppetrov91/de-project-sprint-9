import os
from lib.kafka_client import KafkaConsumer, KafkaProducer
from lib.pg_client import PostgresClient
from lib.redis_client import RedisClient
from typing import Dict
from logging import Logger


class AppConfig:
    @staticmethod
    def __get_env_variables_dict() -> Dict:
        return {
            'CERTIFICATE_PATH': '/usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt',
            'DEFAULT_JOB_INTERVAL': '25',
            'KAFKA_HOST': '',
            'KAFKA_PORT': '9092',
            'KAFKA_USERNAME': '',
            'KAFKA_PASSWORD': '',
            'KAFKA_CONSUMER_GROUP': '',
            'KAFKA_SOURCE_TOPIC': '',
            'TRANSACTIONAL_ID': '',
            'KAFKA_DESTINATION_TOPIC': '',
            'REDIS_HOST': '',
            'REDIS_PORT': '6379',
            'REDIS_PASSWORD': '',
            'PG_WAREHOUSE_HOST': '',
            'PG_WAREHOUSE_PORT': '5432',
            'PG_WAREHOUSE_DBNAME': '',
            'PG_WAREHOUSE_USER': '',
            'PG_WAREHOUSE_PASSWORD': '',
            'BATCH_SIZE': '100'
        }
        
    def __init__(self, logger: Logger) -> None:
        self.__logger = logger

        for k, v in self.__get_env_variables_dict().items():
            val = os.getenv(k) or v

            if k in ('BATCH_SIZE', 'DEFAULT_JOB_INTERVAL', 'KAFKA_PORT', 
                     'REDIS_PORT', 'PG_WAREHOUSE_PORT'):
                val = int(val)

            setattr(self, f"_{self.__class__.__name__}__{k.lower()}", val)

    @property
    def batch_size(self):
        return self.__batch_size
    
    @batch_size.setter
    def batch_size(self, batch_size):
        self.__batch_size = batch_size

    def kafka_producer(self) -> KafkaProducer:
        return KafkaProducer(
            self.__kafka_host,
            self.__kafka_port,
            self.__kafka_username,
            self.__kafka_password,
            self.__kafka_destination_topic,
            self.__certificate_path,
            self.__transactional_id,
            self.__logger
        )

    def kafka_consumer(self) -> KafkaConsumer:
        return KafkaConsumer(
            self.__kafka_host,
            self.__kafka_port,
            self.__kafka_username,
            self.__kafka_password,
            self.__kafka_source_topic,
            self.__kafka_consumer_group,
            self.__certificate_path,
            self.__logger
        )

    def redis_client(self) -> RedisClient:
        return RedisClient(
            self.__redis_host,
            self.__redis_port,
            self.__redis_password,
            self.__certificate_path
        )

    def postgres_client(self) -> PostgresClient:
        return PostgresClient(
            self.__pg_warehouse_host,
            self.__pg_warehouse_port,
            self.__pg_warehouse_dbname,
            self.__pg_warehouse_user,
            self.__pg_warehouse_password
        )
