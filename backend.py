"""all database connection and data processing is done here """

import sys
from mysql.connector import MySQLConnection, Error, errorcode


# ------------ mysql connection, read mysql table description is done here..
def read_mysql_table_description(creds, logging):
    """Reading description of mysql table done here"""

    logging.info("#Step : Reading table description is start")
    try:
        conn = MySQLConnection(
            user=creds["mysql"]["user"], password=creds["mysql"]["pass"], host=creds["mysql"]["host"], database=creds["mysql"]["db"])
        if conn.is_connected():
            logging.info("#Step : Mysql connection established successfully")

            cursor = conn.cursor()
            cursor.execute("SHOW COLUMNS FROM {}".format(creds["mysql"]["table"]))
            return cursor.fetchall()

    except Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            logging.error(
                '#Error : Something is wrong with your user name or password')
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            logging.error('#Error : Database does not exist')
        else:
            logging.error('#Error : %s', err)

        sys.exit(0)

    else:
        conn.close()
