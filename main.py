"""Redshift ddl generator from mysql table"""

from mysql.connector import MySQLConnection, Error
import json
import sys
import logging

LOG_FILENAME = 'work.log'
logging.basicConfig(filename=LOG_FILENAME, level=logging.DEBUG)
logging.getLogger().addHandler(logging.StreamHandler())


def read_conf(conf_file):
    """Reading conf done here"""

    logging.info("#Step : Reading conf file is start")
    with open(conf_file) as json_data_file:
        return json.load(json_data_file)


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


def generate_ddl(table_info, creds):
    "Form mysql table info DDL generation is done here"

    logging.info("#Step : DDL generation is start...")
    print type(table_info)
    print table_info

    text_file = open("redshift_ddl.sql", "w")
    text_file.write("CREATE TABLE {0}.{1}(\n);".format(creds["redshift"]["schema"], creds["mysql_table"]))
    for row in table_info:
        print row[0], "\t", row[1]
        
    text_file.close()


def generate_ddl_start(file_name):
    "All steps for query generations"

    creds = read_conf(file_name)
    table_info = read_mysql_table_description(creds)
    generate_ddl(table_info, creds)


if __name__ == '__main__':

    generate_ddl_start("conf.json")
