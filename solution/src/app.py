import logging
import sys
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask
from lib.app_config import AppConfig
from solution.src.dds_service.dds_message_processor_job import DDSMessageProcessor
from solution.src.stg_service.stg_message_processor_job import StgMessageProcessor

app = Flask(__name__)


@app.get('/health')
def health():
    return 'healthy'

def set_logger():
    logger = app.logger
    logger.setLevel(logging.INFO)
    ch = logging.StreamHandler()

    formatter = logging.Formatter('dt=%(asctime)s,  name=%(name)s, level=%(levelname)s, msg=%(message)s',  
                                  datefmt="%Y-%m-%d %H:%M:%S")
    ch.setFormatter(formatter)
    logger.addHandler(ch)

def run_service():
    d = {
        'consumer': None,
        'postgres': None,
        'redis': None,
        'producer': None,
        'batch_size': config.batch_size,
        'logger': app.logger
    }

    try:
        service_type = sys.argv[1]
        obj_type = (StgMessageProcessor, DDSMessageProcessor)[service_type == "dds"]
        d['consumer'] = config.kafka_consumer(service_type)
        d['producer'] = config.kafka_producer(service_type)

        if service_type == 'stg':
            d['redis'] = config.redis_client()

        d['postgres'] = config.postgres_client()
        proc = obj_type(**d)

        scheduler = BackgroundScheduler()
        scheduler.add_job(func=proc.run, trigger="interval", seconds=config.default_job_interval)
        scheduler.start()

        app.run(host='0.0.0.0', use_reloader=False)
    except Exception as err:
        app.logger.error(err, stack_info=True, exc_info=True)
    finally:
        for k in d.keys():
            if k not in ('batch_size', 'producer', 'logger') and d[k]:
                d[k].close()


if __name__ == '__main__':
    set_logger()
    config = AppConfig(app.logger)
    run_service()