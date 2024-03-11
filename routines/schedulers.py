# This file contains the schedulers for the application

from routes import front_interaction
import logging


logging.basicConfig(level=logging.INFO)


def update_from_db1c():
    for table in front_interaction.TABLES.keys():
        front_interaction.update_data(table)
        logging.info(f"{table} updated")
