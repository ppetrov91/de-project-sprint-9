import logging
import sys
from lib.app_config import AppConfig
from stg_service.stg_message_processor_job import StgMessageProcessor
from dds_service.dds_message_processor_job import DDSMessageProcessor


def set_logger():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    ch = logging.StreamHandler()

    formatter = logging.Formatter('dt=%(asctime)s,  name=%(name)s, level=%(levelname)s, msg=%(message)s',  
                                  datefmt="%Y-%m-%d %H:%M:%S")
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger

def run_service():
    d = {
        'consumer': None,
        'postgres': None,
        'producer': None,
        'batch_size': config.batch_size,
        'logger': logger
    }

    try:
        service_type = sys.argv[1]
        obj_type = (StgMessageProcessor, DDSMessageProcessor)[service_type == "dds"]
        d['consumer'] = config.kafka_consumer(service_type)
        d['producer'] = config.kafka_producer(service_type)

        if service_type == "stg":
            d['redis'] = config.redis_client()

        d['postgres'] = config.postgres_client()
        proc = obj_type(**d)
        proc.run()
    except Exception as err:
        logger.error(err, stack_info=True, exc_info=True)
    finally:
        for k in d.keys():
            if k not in ('batch_size', 'producer', 'logger') and d[k]:
                d[k].close()

if __name__ == '__main__':
    logger = set_logger()
    config = AppConfig(logger)
    run_service()