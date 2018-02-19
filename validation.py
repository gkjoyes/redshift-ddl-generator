"""
Validation for user inputs
"""


def validate(params, logging):
    """
    Validate user inputs
    """

    # ----mysql
    if params.get("mysql"):

        # ----host
        if not params.get("mysql").get("host").strip():
            return "provide mysql host"

        # ----db
        if not params.get("mysql").get("db").strip():
            return "please provide mysql db"

        # ----table
        if not params.get("mysql").get("table").strip():
            return "please provide mysql table"

        # ----username
        if not params.get("mysql").get("user").strip():
            return "provide username for mysql conn"

        # ----password
        if not params.get("mysql").get("pass").strip():
            return "provide password for mysql conn"

    else:
        return "provide required params for mysql"

    # ----redshift
    if params.get("redshift"):

        # ----schema
        if not params.get("redshift").get("schema").strip():
            return "provide redshift schema"
    else:
        return "provide required params for redshift"

    return None
