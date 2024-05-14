import os
from lib.kafka_client import KafkaConsumer, KafkaProducer
from lib.pg_client import PostgresClient
from lib.redis_client import RedisClient
from typing import Dict, Tuple
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
            'KAFKA_STG_CONSUMER_GROUP': '',
            'KAFKA_STG_SRC_TOPIC': '',
            'KAFKA_STG_DST_TOPIC': '',
            'STG_TRANSACTIONAL_ID': '',
            'KAFKA_DDS_CONSUMER_GROUP': '',
            'KAFKA_DDS_SRC_TOPIC': '',
            'KAFKA_DDS_DST_TOPIC': '',
            'DDS_TRANSACTIONAL_ID': '',
            'KAFKA_CDM_CONSUMER_GROUP': '',
            'KAFKA_CDM_SRC_TOPIC': '',
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

    def __get_attrs(self, service_type: str) -> Tuple:
        return (
            (self.__kafka_stg_src_topic, 
             self.__kafka_stg_dst_topic, 
             self.__kafka_stg_consumer_group,
             self.__stg_transactional_id
            ),
            (self.__kafka_dds_src_topic,
             self.__kafka_dds_dst_topic, 
             self.__kafka_dds_consumer_group, 
             self.__dds_transactional_id
            ),
            (self.__kafka_cdm_src_topic, 
             None,
             self.__kafka_cdm_consumer_group,
             None
            )
        )[(service_type == "dds") * 1 or (service_type == "cdm") * 2]

    def kafka_producer(self, service_type: str) -> KafkaProducer:
        if service_type not in ("stg", "dds"):
            raise Exception("A KafkaProducer instances are created for stg or dds service type")

        _, self.__kafka_dst_topic, _, self.__transactional_id = self.__get_attrs(service_type)

        return KafkaProducer(
            host=self.__kafka_host,
            port=self.__kafka_port,
            user=self.__kafka_username,
            password=self.__kafka_password,
            topic=self.__kafka_dst_topic,
            cert_path=self.__certificate_path,
            transactional_id=self.__transactional_id,
            logger=self.__logger
        )

    def kafka_consumer(self, service_type: str) -> KafkaConsumer:
        self.__kafka_src_topic, _, self.__kafka_consumer_group, _ = self.__get_attrs(service_type)

        return KafkaConsumer(
            host=self.__kafka_host,
            port=self.__kafka_port,
            user=self.__kafka_username,
            password=self.__kafka_password,
            topic=self.__kafka_src_topic,
            group=self.__kafka_consumer_group,
            cert_path=self.__certificate_path,
            logger=self.__logger
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