import logging
import sys

lib_path = sys.path[0]
lib_path = '/'.join(lib_path.split('/')[:-2])
sys.path.append(lib_path)

from lib.app_config import AppConfig
from service_stg.stg_message_processor_job import StgMessageProcessor


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    config = AppConfig(logger)
    
    d = {
        'consumer': None,
        'redis': None,
        'postgres': None,
        'producer': None,
        'batch_size': config.batch_size,
        'logger': logger
    }

    try:
        d['consumer'] = config.kafka_consumer()
        d['producer'] = config.kafka_producer()
        d['redis'] = config.redis_client()
        d['postgres'] = config.postgres_client()
        proc = StgMessageProcessor(**d)
        proc.run()
    except Exception as err:
        logger.error(err, stack_info=True, exc_info=True)
    finally:
        for k in d.keys():
            if k not in ('batch_size', 'producer', 'logger') and d[k]:
                d[k].close()