# Проект 9-го спринта

### Структура проекта
1. app - helm-chart для создания трёх сервисов.
2. ddl - SQL-скрипты создания объектов схем stg, dds и cdm.
3. src/cdm_service - код cdm-service для наполнения витрин данных.
4. src/dds_service - код dds-service для наполнения dds-слоя.
5. src/lib - код классов KafkaProducer, KafkaConsumer, PostgresClient, RedisClient. Используются в трёх сервисах, поэтому вынесены в отдельную директорию.
6. src/stg_service - код stg-service для наполнения stg-слоя.
7. src/app.py - главный файл запуска сервисов.
8. docker-compose.yaml - файл разворачивания контейнеров для сервисов.
9. dockerfile - файл для создания контейнера под сервис.

### Описание класса AppConfig файла lib/app_config.py
1. Метод __get_env_variables_dict() - возвращает словарь конфигурационных параметров:
    - CERTIFICATE_PATH - путь к сертификату для подключения к компонентам Yandex Cloud.
	- DEFAULT_JOB_INTERVAL - частота работы сервиса. Например, раз в две минуты.
	- KAFKA_HOST - наименование хоста с брокером Kafka.
	- KAFKA_PORT - порт брокера Kafka.
	- KAFKA_USERNAME - логин подключения к брокеру Kafka.
	- KAFKA_PASSWORD - пароль подключения к брокеру Kafka.
	- KAFKA_STG_CONSUMER_GROUP - consumer group для чтения данных из Kafka сервисом stg-service.
	- KAFKA_STG_SRC_TOPIC - топик для чтения данных сервисом stg-service.
	- KAFKA_STG_DST_TOPIC - топик, в который stg-service будет записывать данные.
	- STG_TRANSACTIONAL_ID - transactional_id для записи данных в topic stg-сервисом с использованием механизма транзакций.
	- KAFKA_DDS_CONSUMER_GROUP - consumer group для чтения данных из Kafka сервисом dds-service.
	- KAFKA_DDS_SRC_TOPIC - топик для чтения данных сервисом dds-service.
    - KAFKA_DDS_DST_TOPIC - топик для записи данных сервисом dds-service.
    - DDS_TRANSACTIONAL_ID - transactional_id для записи данных в topic dds-сервисом с использованием механизма транзакций.
    - KAFKA_CDM_CONSUMER_GROUP - consumer group для чтения данных из Kafka сервисом cdm-service.
    - KAFKA_CDM_SRC_TOPIC - топик для чтения данных сервисом cdm-service.
    - REDIS_HOST - хост СУБД Redis.
    - REDIS_PORT - порт СУБД Redis.
    - REDIS_PASSWORD - пароль для подключения к СУБД Redis. 
    - PG_WAREHOUSE_HOST - хост СУБД PostgreSQL.
    - PG_WAREHOUSE_PORT - порт СУБД PostgreSQL.
    - PG_WAREHOUSE_DBNAME - наименование БД в СУБД PostgreSQL.
    - PG_WAREHOUSE_USER - имя пользователя для подключения к СУБД PostgreSQL.
    - PG_WAREHOUSE_PASSWORD - пароль пользователя для подключения к СУБД PostgreSQL.
    - BATCH_SIZE - число забираемых сообщений из топика Kafka за один проход.

2. Конструктор класса AppConfig:
    - Создаёт private атрибуты на основе словаря, возвращаемого методом __get_env_variables_dict().
	- batch_size, default_job_interval и порты компонентов Yandex Cloud преобразуются в целое число.

3. default_job_interval() - getter и setter для обращения к атрибуту __default_job_interval.

4. batch_size() - getter и setter для обращения к атрибуту __batch_size.

5. private метод get_attrs() - возвращает атрибуты, необходимые для создания KafkaConsumer и KafkaProducer.
    - Топик чтения данных.
	- Топик записи данных.
	- Consumer group для чтения данных.
	- transactional id для записи данных с использованием механизма транзакции.

	Для cdm сервиса топик записи данных и transactional id is None, поскольку он только читает данные.

6. kafka_producer() - метод создания экземпляра класса KafkaProducer.

7. kafka_consumer() - метод создания экземпляра класса KafkaConsumer.

8. redis_client() - метод создания экземпляра класса RedisClient.

9. postgres_client() - метод создания экземпляра класса PostgresClient.

### Описание класса KafkaProducerConsumer файла lib/kafka_client.py
Абстрактный класс, является родительским для KafkaConsumer и KafkaProducer.


1. _error_callback() - callback, вызываемый при возникновении ошибки при работе с Kafka broker.

2. _get_params() - метод, возвращающий словарь конфигурационных параметров подключения к Kafka broker.

3. _create_client() - абстрактный метод, переопределяется в KafkaConsumer и KafkaProducer.

4. _exec_func() - вспомогательная функция для вызова:
   - produce()
   - consume()
   - begin_transaction()
   - commit_transaction()
   - abort_transaction()
   - commit()

   Иногда может быть инициировано исключение KafkaError, в котором содержится информация о возможности повторения той или иной операции. Если есть возможность, то пытаемся повторить операцию, но не более attempts раз. Если операцию не получается выполнить attempts раз подряд, то инициируем исключение.

5. Конструктор класса KafkaProducerConsumer. Принимает следующие параметры:
   - host - host Kafka broker
   - port - порт Kafka broker
   - user - логин подключения к Kafka broker.
   - password - пароль подключения к Kafka broker.
   - topic - топик подключения к Kafka broker.
   - cert_path - путь к сертификату для подключения к Kafka broker.
   - logger - logger для протоколирования действий сервиса с последующей записью в файл.
   - timeout - timeout любой операции с kafka broker, 10 секунд.
   - attempts - максимальное число любой операции с kafka broker, 10.