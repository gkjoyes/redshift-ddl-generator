"""Redshift ddl generator from mysql table"""

from mysql.connector import MySQLConnection, Error
import json
import sys

def read_conf(conf_file):
    """Reading conf done here"""

    with open(conf_file) as json_data_file:
        return json.load(json_data_file)


def read_mysql_table_description(creds):
    """Reading description of mysql table done here"""

    try:
        CONN = MySQLConnection(**creds["mysql"])
        if CONN.is_connected():
            print "Connected to mysql database"

            cursor = CONN.cursor()
            cursor.execute("SHOW COLUMNS FROM employees")
            return cursor.fetchall()

    except Error as e:
        print e
        sys.exit(1)

    finally:
        CONN.close()


if __name__ == '__main__':

    cred = read_conf("conf.json")
    res = read_mysql_table_description(cred)

    print res
    