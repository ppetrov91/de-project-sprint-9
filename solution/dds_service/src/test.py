import logging
import sys

lib_path = sys.path[0]
lib_path = '/'.join(lib_path.split('/')[:-2])
sys.path.append(lib_path)

from lib.app_config import AppConfig
from dds_service.src.dds_loader.dds_message_processor_job import DDSMessageProcessor


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    config = AppConfig(logger)
    
    d = {
        'consumer': None,
        'postgres': None,
        'producer': None,
        'batch_size': config.batch_size,
        'logger': logger
    }

    try:
        d['consumer'] = config.kafka_consumer("dds")
        d['producer'] = config.kafka_producer("dds")
        d['postgres'] = config.postgres_client()
        proc = DDSMessageProcessor(**d)
        proc.run()
    except Exception as err:
        logger.error(err, stack_info=True, exc_info=True)
    finally:
        for k in d.keys():
            if k not in ('batch_size', 'producer', 'logger') and d[k]:
                d[k].close()