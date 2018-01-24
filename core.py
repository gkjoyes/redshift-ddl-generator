"""Core functionality for ddl generation is done here"""


import json
import re
import sys


# --------------generation of ddl corresponding to redshift is done here..
def generate_ddl(table_info, creds, datatype_mapping, logging):
    "Form mysql table info DDL generation is done here"

    logging.info("#Step : DDL generation is start...")

    # ---------result file
    text_file = open("redshift_ddl.sql", "w")

    # -------- data mapping types
    try:
        original_data_type = json.load(open(datatype_mapping))
    except Exception as err:
        logging.warning("#Error : " + str(err))
        sys.exit(0)

    # --------start
    text_file.write("CREATE TABLE {0}.{1}(".format(
        creds["redshift"]["schema"], creds["mysql"]["table"]))

    last_iteration = len(table_info) - 1
    # --------table columns descriptions
    for i, row in enumerate(table_info):

        text_file.write("\n\t")
        # ----------- redshift: column name
        text_file.write(re.sub('(?<!^)(?=[A-Z])', '_', row[0]).lower())

        # ---------- redshift : datatype
        text_file.write(
            "\t" + original_data_type[re.search(r'^\w+', row[1]).group(0)])

        # --------- if datatype is enum
        if 'enum' in str(row[1]):

            enum_values = list(
                x for x in row[1][4:] if x != '\'' and x != ',' and x != '(' and x != ')')

            enum_max_len = len(max(enum_values))
            text_file.write("(" + str(enum_max_len * 2) + ")")

        # ------ if datatype is varchar
        elif 'varchar' in str(row[1]):
            text_file.write(re.search(r'\(.*\)', row[1]).group(0))

        if i != last_iteration:
            text_file.write(",")

    text_file.write("\n" + ");")
    text_file.close()
    logging.info("#Step : DDL generation completed...")
