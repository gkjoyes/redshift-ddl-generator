"""Redshift ddl generator from mysql table"""

import json
import logging
import sys
from backend import read_mysql_table_description
from core import generate_ddl

# ------------- logging level..
LOG_FILENAME = 'work.log'
CONF_FILE = 'config/conf.json'
DATA_TYPE_MAPPING = 'config/data_type_mapping.json'
logging.basicConfig(filename=LOG_FILENAME, level=logging.DEBUG)
logging.getLogger().addHandler(logging.StreamHandler())


# ------------- read configuration file..
def read_conf(conf_file):
    """Reading conf done here"""

    logging.info("#Step : Reading conf file is start")
    try:
        with open(conf_file) as json_data_file:
            return json.load(json_data_file)
    except:
        logging.info("#Error : Something went rough while reding conf file..")
        sys.exit(0)


# ------------main steps for ddl generation..
def generate_ddl_start():
    "All steps for query generations"

    creds = read_conf(CONF_FILE)
    table_info = read_mysql_table_description(creds, logging)
    generate_ddl(table_info, creds, DATA_TYPE_MAPPING, logging)


# ------------main..
if __name__ == '__main__':

    generate_ddl_start()
