import json
from logging import Logger
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
from abc import ABC, abstractmethod


class KafkaProducerConsumer(ABC):
    @staticmethod
    def _error_callback(err):
        print(f'An error was occured: {err}')

    @staticmethod
    def _get_params(host, port, cert_path, user, password):
        return {
            'bootstrap.servers': f'{host}:{port}',
            'security.protocol': 'SASL_SSL',
            'ssl.ca.location': cert_path,
            'sasl.mechanism': 'SCRAM-SHA-512',
            'sasl.username': user,
            'sasl.password': password,
            'error_cb': KafkaProducerConsumer._error_callback,
        }
    
    @abstractmethod
    def _create_client(self):
        pass

    @staticmethod
    def _exec_func(attempts, func, func_args, func_kwargs, logger):
        for _ in range(attempts):
            try:
                res = func(*func_args, **func_kwargs)
                return res
            except Exception as err:
                logger.error(err, stack_info=True, exc_info=True)

                if type(err) == KafkaException:
                    err = err.args[0]

                if type(err) != KafkaError or not err.retriable():
                    raise err

        raise err

    def __init__(self,
                 host: str,
                 port: int,
                 user: str,
                 password: str,
                 topic: str,
                 cert_path: str,
                 logger: Logger,
                 timeout: float = 60.0,
                 attempts: int = 10
                ):
        self._params = self._get_params(host, port, cert_path, user, password)
        self._topic = topic
        self._attempts = attempts
        self._timeout = timeout
        self._logger = logger


class KafkaProducer(KafkaProducerConsumer):
    def _create_client(self):
        self.__client = Producer(self._params)
        self._exec_func(attempts=self._attempts,
                        func=self.__client.init_transactions, 
                        func_args=(self._timeout,),
                        func_kwargs={},
                        logger=self._logger
                       )

    def __begin_transaction(self):
        self._exec_func(attempts=self._attempts,
                        func=self.__client.begin_transaction,
                        func_args=tuple(),
                        func_kwargs={}, 
                        logger=self._logger
                       )

    def __commit_transaction(self):
        self._exec_func(attempts=self._attempts,
                        func=self.__client.commit_transaction,
                        func_args=(self._timeout,),
                        func_kwargs={},
                        logger=self._logger
                       )
    
    def __abort_transaction(self):
        self._exec_func(attempts=self._attempts,
                        func=self.__client.abort_transaction,
                        func_args=(self._timeout,),
                        func_kwargs={}, 
                        logger=self._logger
                       )

    def __init__(self,
                 host: str,
                 port: int,
                 user: str,
                 password: str,
                 topic: str,
                 cert_path: str,
                 transactional_id: str,
                 logger: Logger,
                 timeout: float = 60.0,
                 attempts: int = 10
                ):
        super().__init__(host, port, user, password, 
                         topic, cert_path, logger, timeout, attempts)

        self._params['transactional.id'] = transactional_id
        self._create_client()
        
    def __produce(self, payload):
        self._exec_func(attempts=self._attempts,
                        func=self.__client.produce,
                        func_args=[self._topic],
                        func_kwargs={"value": json.dumps(payload)},
                        logger=self._logger)
        
    def save_data_to_kafka(self, data):
        self._logger.info("Start saving data to kafka")
        self.__begin_transaction()
        
        try:
            for mes in data:
                self.__produce(mes)
            self.__commit_transaction()
        except Exception as err:
            self._logger.error("Error while saving data to kafka")
            self.__abort_transaction()
            raise err
        finally:
            self._logger.info("Stop saving data to kafka")


class KafkaConsumer(KafkaProducerConsumer):
    def close(self):
        if self.__client:
            self.__client.close()

    def _create_client(self):
        self.__client = Consumer(self._params)
        self.__client.subscribe([self._topic])

    def commit(self):
        self._exec_func(attempts=self._attempts,
                        func=self.__client.commit,
                        func_args=tuple(),
                        func_kwargs={},
                        logger=self._logger
                       )

    def __init__(self,
                 host: str,
                 port: int,
                 user: str,
                 password: str,
                 topic: str,
                 group: str,
                 cert_path: str,
                 logger: Logger,
                 timeout: float = 60.0,
                 attempts: int = 10
                ):
        super().__init__(host, port, user, password,
                         topic, cert_path, logger, timeout, attempts)
        
        self._params['auto.offset.reset'] = 'earliest'
        self._params['group.id'] = group
        self._params['enable.auto.commit'] = False
        self._create_client()

    def consume(self, batch_size=100):
        return self._exec_func(attempts=self._attempts,
                               func=self.__client.consume,
                               func_args=tuple(),
                               func_kwargs={"num_messages": batch_size, 
                                            "timeout": self._timeout},
                               logger=self._logger)