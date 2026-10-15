import logging
from datetime import datetime

# Configuração básica de logging para uso nos módulos
logging.basicConfig(
    format='%(asctime)s %(levelname)s [%(filename)s]: %(message)s',
    level=logging.INFO
)


def log_info(msg):
    logging.info(msg)


def log_warning(msg):
    logging.warning(msg)


def log_error(msg):
    logging.error(msg)


def log_pipeline_start(pipeline_name):
    logging.info(f"Iniciando execução do pipeline: {pipeline_name} - {datetime.now()}")


def log_pipeline_end(pipeline_name):
    logging.info(f"Finalizando execução do pipeline: {pipeline_name} - {datetime.now()}")
