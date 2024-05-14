import logging
import sys

lib_path = sys.path[0]
lib_path = '/'.join(lib_path.split('/')[:-2])
sys.path.append(lib_path)


from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask

from lib.app_config import AppConfig
from dds_service.src.dds_loader.dds_message_processor_job import DDSMessageProcessor

app = Flask(__name__)


@app.get('/health')
def health():
    return 'healthy'


if __name__ == '__main__':
    app.logger.setLevel(logging.INFO)
    config = AppConfig(app.logger)
    
    d = {
        'consumer': None,
        'postgres': None,
        'producer': None,
        'batch_size': config.batch_size,
        'logger': app.logger
    }

    try:
        d['consumer'] = config.kafka_consumer("dds")
        d['producer'] = config.kafka_producer("dds")
        d['postgres'] = config.postgres_client()
        proc = DDSMessageProcessor(**d)

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