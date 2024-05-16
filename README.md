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

### Описание класса AppConfig файла app_config.py
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

2. Метод __init__() - конструктор класса AppConfig:
    - Создаёт private атрибуты на основе словаря, возвращаемого методом __get_env_variables_dict().
	- batch_size, default_job_interval и порты компонентов Yandex Cloud преобразуются в целое число.

3. default_job_interval() - getter и setter для обращения к атрибуту __default_job_interval.

4. batch_size() - getter и setter для обращения к атрибуту __batch_size.

5. 

