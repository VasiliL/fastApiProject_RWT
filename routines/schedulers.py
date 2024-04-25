# This file contains the schedulers for the application

from routes import front_interaction
from components.logger_config import configure_logger
import logging

configure_logger()


def update_from_db1c():
    logging.info(f"update_from_db1c started")
    for table in front_interaction.TABLES.keys():
        front_interaction.update_data(table)
        logging.info(f"{table} updated")
