"""Core processing for ddl generation"""

import json
import re
import sys


def generate_ddl(table_info, creds, datatype_mapping, logging):
    """
    DDL generation is done with help of table description
    """

    logging.info("#Step: DDL generation is start...")
    # ----file to write final ddl
    text_file = open("redshift_ddl.sql", "w")

    # ----data types mapping
    try:
        with open(datatype_mapping) as f:
            data_types = json.load(f)
    except Exception as err:
        logging.warning("#Error: ", err)
        sys.exit(1)

    # ----start
    text_file.write("CREATE TABLE {0}.{1}(".format(
        creds["redshift"]["schema"], creds["mysql"]["table"]))

    last_iteration = len(table_info) - 1

    # ----go through table descriptions
    for i, row in enumerate(table_info):

        text_file.write("\n\t")
        # ----redshift: column name
        text_file.write(re.sub('(?<!^)(?=[A-Z])', '_', row[0]).lower())

        # ----redshift: datatype
        text_file.write(
            "\t" + data_types[re.search(r'^\w+', row[1]).group(0)])

        # ----if datatype is enum
        if 'enum' in str(row[1]):
            enum_values = tuple(
                x for x in row[1][4:]
                if x != '\'' and x != ',' and x != '(' and x != ')')

            enum_max_len = len(max(enum_values))
            text_file.write("(" + str(enum_max_len * 2) + ")")

        # ----if datatype is varchar
        elif 'varchar' in str(row[1]):
            text_file.write(re.search(r'\(.*\)', row[1]).group(0))

        if i != last_iteration:
            text_file.write(",")

    text_file.write("\n" + ");")
    text_file.close()
    logging.info("#Step: DDL generation completed...")
