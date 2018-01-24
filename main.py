"""Redshift ddl generator from mysql table"""

import json
import logging
import sys
from backend import read_mysql_table_description
from core import generate_ddl

# ------------- logging level..
LOG_FILENAME = 'work.log'
logging.basicConfig(filename=LOG_FILENAME, level=logging.DEBUG)
logging.getLogger().addHandler(logging.StreamHandler())


# ------------- read configuration file..
def read_conf(conf_file):
    """Reading conf done here"""

    logging.info("#Step : Reading conf file is start")
    try:
        return json.load(open(conf_file))
    except Exception as err:
        logging.info("#Error : "+str(err))
        sys.exit(0)

# ------------main steps for ddl generation..
def generate_ddl_start():
    "All steps for query generations"

    # ------- file names
    conf_file = 'config/conf.json'
    data_type_mapping = 'config/data_type_mapping.json'

    #-------- processing steps
    creds = read_conf(conf_file)
    table_info = read_mysql_table_description(creds, logging)
    generate_ddl(table_info, creds, data_type_mapping, logging)


# ------------main..
if __name__ == '__main__':

    generate_ddl_start()
