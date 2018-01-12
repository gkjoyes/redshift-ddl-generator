"""all database connection and data processing is done here """

import sys
from mysql.connector import MySQLConnection, Error


# ------------ mysql connection, read mysql table description is done here..
def read_mysql_table_description(creds, logging):
    """Reading description of mysql table done here"""

    logging.info("#Step : Reading table description is start")
    try:
        conn = MySQLConnection(**creds["mysql"])
        if conn.is_connected():
            logging.info("#Step : Mysql connection established successfully")

            cursor = conn.cursor()
            cursor.execute("SHOW COLUMNS FROM {}".format(creds["mysql_table"]))
            return cursor.fetchall()

    except Error as err:
        logging.error('#Error : Connection error %s', err.errno)
        sys.exit(1)

    finally:
        conn.close()
