"""Core processing for ddl generation"""

import json
import re
import sys


def generate_ddl(table_info, creds, datatype_mapping, logging):
    """
    DDL generation is done with help of table description
    """

    logging.info("#Step: DDL generation is start...")
    # Initialize output file.
    text_file = open("redshift_ddl.sql", "w")

    # Data types mapping.
    try:
        with open(datatype_mapping) as f:
            data_types = json.load(f)
    except Exception as err:
        logging.warning("#Error: ", err)
        sys.exit(1)

    # Start ddl generation.
    text_file.write("CREATE TABLE {0}.{1}".format(
        creds["redshift"]["schema"], creds["mysql"]["table"]))

    last_iteration = len(table_info) - 1

    # Go through table descriptions.
    text_file.write("(")
    for i, row in enumerate(table_info):

        text_file.write("\n\t")
        # Redshift: column name.
        text_file.write(re.sub('(?<!^)(?=[A-Z])', '_', row[0]).lower())

        # Redshift: datatype.
        text_file.write(
            "\t" + data_types[re.search(r'^\w+', row[1]).group(0)])

        # If datatype is enum.
        if 'enum' in str(row[1]):
            enum_values = tuple(
                x for x in row[1][4:]
                if x != '\'' and x != ',' and x != '(' and x != ')')

            enum_max_len = len(max(enum_values))
            text_file.write("(" + str(enum_max_len * 2) + ")")

        # If datatype is varchar.
        elif 'varchar' in str(row[1]):
            text_file.write(re.search(r'\(.*\)', row[1]).group(0))

        if i != last_iteration:
            text_file.write(",")

    text_file.write("\n" + ");")
    text_file.close()
    logging.info("#Step: DDL generation completed...")
