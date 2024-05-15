import logging
import sys
import logging.handlers as handlers
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask
from lib.app_config import AppConfig


app = Flask(__name__)


@app.get('/health')
def health():
    return 'healthy'

def set_logger():
    logger = app.logger
    logger.setLevel(logging.INFO)
    handler = handlers.TimedRotatingFileHandler(filename="/log/app.log", 
                                                when="H", 
                                                interval=24,
                                                backupCount=7)

    formatter = logging.Formatter('dt=%(asctime)s,  name=%(name)s, level=%(levelname)s, msg=%(message)s',  
                                  datefmt="%Y-%m-%d %H:%M:%S")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

def create_proc(service_type):
    d = {
        'consumer': None,
        'postgres': None,
        'redis': None,
        'producer': None,
        'batch_size': config.batch_size,
        'logger': app.logger
    }

    d['consumer'] = config.kafka_consumer(service_type)
    d['postgres'] = config.postgres_client()

    if service_type == "stg":
        d['producer'] = config.kafka_producer(service_type)
        d['redis'] = config.redis_client()
        from stg_service.stg_message_processor_job import StgMessageProcessor
        obj_type = StgMessageProcessor
    elif service_type == "dds":
        d['producer'] = config.kafka_producer(service_type)
        from dds_service.dds_message_processor_job import DDSMessageProcessor
        obj_type = DDSMessageProcessor
    else:
        from cdm_service.cdm_message_processor_job import CDMMessageProcessor
        obj_type = CDMMessageProcessor

    return d, obj_type(**d)

def run_service():
    try:
        service_type = sys.argv[1]

        if service_type not in ("stg", "dds", "cdm"):
            raise Exception("service type must be stg, dds or cdm")

        d, proc = create_proc(service_type)
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