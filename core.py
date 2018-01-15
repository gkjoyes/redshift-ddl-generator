"""Core functionality for ddl generation is done here"""


import json
import re


# --------------generation of ddl corresponding to redshift is done here..
def generate_ddl(table_info, creds, datatype_mapping, logging):
    "Form mysql table info DDL generation is done here"

    logging.info("#Step : DDL generation is start...")

    # ---------result file
    text_file = open("redshift_ddl.sql", "w")

    with open(datatype_mapping) as data_types:
        original_data_type = json.load(data_types)

    # --------start
    text_file.write("CREATE TABLE {0}.{1}(".format(
        creds["redshift"]["schema"], creds["mysql_table"]))

    # --------table columns
    for i, row in enumerate(table_info):

        text_file.write("\n\t")

        # ----------- column name compactability
        text_file.write(re.sub('(?<!^)(?=[A-Z])', '_', row[0]).lower())

        # ---------- extract datatype
        index = row[1].find('(')
        if index >= 0:
            data_type = row[1][:index]
        else:
            data_type = row[1]

        text_file.write("\t" + original_data_type[data_type])

        # --------- datatype: enum
        if 'enum' in str(row[1]):

            enum_values = list(
                x for x in row[1][4:] if x != '\'' and x != ',' and x != '(' and x != ')')

            enum_max_len = len(max(enum_values))
            text_file.write("(" + str(enum_max_len * 2) + ")")

        elif 'varchar' in str(row[1]):
            index_1 = row[1].find('(')
            if index >= 0:
                varchar_postfix = row[1][index_1:]

            text_file.write(varchar_postfix)

        if i != len(table_info) - 1:
            text_file.write(",")

    text_file.write("\n" + ");")
    text_file.close()
    logging.info("#Step : DDL generation completed...")
