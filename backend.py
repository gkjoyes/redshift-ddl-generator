"""database connections and data processing"""

import sys
from mysql.connector import MySQLConnection, Error, errorcode


def read_mysql_table_description(creds, logging):
    """
    Reading description of mysql table
    """

    logging.info("#Step: Reading table description is start")
    try:
        # Connect with MySQL.
        conn = MySQLConnection(
            user=creds["mysql"]["user"],
            password=creds["mysql"]["pass"],
            host=creds["mysql"]["host"],
            database=creds["mysql"]["db"])

        # Check connection status.
        if conn.is_connected():
            logging.info("#Step: Mysql connection is established")

            cursor = conn.cursor()
            cursor.execute("SHOW COLUMNS FROM {}".format(
                creds["mysql"]["table"]))
            return cursor.fetchall()

    except Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            logging.error('#Error: Incorrect username or password')
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            logging.error('#Error: Database does not exist')
        else:
            logging.error('#Error: %s', err)

        sys.exit(0)

    else:
        conn.close()
