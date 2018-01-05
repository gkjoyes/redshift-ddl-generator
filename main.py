"""Redshift ddl generator from mysql table"""

import json
import sys
import logging
import re
from mysql.connector import MySQLConnection, Error


# ------------- logging level..
LOG_FILENAME = 'work.log'
logging.basicConfig(filename=LOG_FILENAME, level=logging.DEBUG)
logging.getLogger().addHandler(logging.StreamHandler())


# ------------- read configuration file..
def read_conf(conf_file):
    """Reading conf done here"""

    logging.info("#Step : Reading conf file is start")
    with open(conf_file) as json_data_file:
        return json.load(json_data_file)


# ------------ mysql connection, read mysql table description is done here..
def read_mysql_table_description(creds):
    """Reading description of mysql table done here"""

    logging.info("#Step : Reading table description is start")
    try:
        conn = MySQLConnection(**creds["mysql"])
        if conn.is_connected():
            logging.info("#Step : Mysql connection established successfully")

            cursor = conn.cursor()
            cursor.execute("SHOW COLUMNS FROM {}".format(creds["mysql_table"]))
            return cursor.fetchall()

    except Error as e:
        logging.error("#Error : Connection error {}".format(e.errno))
        sys.exit(1)

    finally:
        conn.close()


# --------------generation of ddl corresponding to redshift is done here..
def generate_ddl(table_info, creds, datatype_mapping):
    "Form mysql table info DDL generation is done here"

    logging.info("#Step : DDL generation is start...")
    print type(table_info)
    print table_info

    # ---------result file
    text_file = open("redshift_ddl.sql", "w")

    with open(datatype_mapping) as data_types:
        original_data_type = json.load(data_types)

    # --------start
    text_file.write("CREATE TABLE {0}.{1}(".format(
        creds["redshift"]["schema"], creds["mysql_table"]))

    # --------table columns
    for row in table_info:

        text_file.write("\n\t")
        text_file.write(re.sub('(?<!^)(?=[A-Z])', '_', row[0]).lower())

        index = row[1].find('(')
        if index >= 0:
            data_type = row[1][:index]
        else:
            data_type = row[1]

        text_file.write("\t" + original_data_type[data_type])
        # --------- datatype: enum
        if 'enum' in str(row[1]):
            enum_value = list(filter(lambda a: (
                a != '\'' and a != ',' and a != '(' and a != ')'), (row[1][4:])))

            enum_max_len = len(max(enum_value))

        print row[0], "\t", row[1]
    
    
    text_file.write("\n" + ");")
    text_file.close()


# ------------main steps for ddl generation..
def generate_ddl_start(file_name):
    "All steps for query generations"

    creds = read_conf(file_name)
    table_info = read_mysql_table_description(creds)
    generate_ddl(table_info, creds, "data_type_mapping.json")


# ------------main..
if __name__ == '__main__':

    generate_ddl_start("conf.json")
