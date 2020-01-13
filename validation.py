"""
Validation for user inputs
"""


def validate(params, logging):
    """
    Validate user inputs
    """

    if params.get("mysql"):
        if not params.get("mysql").get("host").strip():
            return "provide mysql host"

        if not params.get("mysql").get("db").strip():
            return "please provide mysql db"

        if not params.get("mysql").get("table").strip():
            return "please provide mysql table"

        if not params.get("mysql").get("user").strip():
            return "provide username for mysql conn"

        if not params.get("mysql").get("pass").strip():
            return "provide password for mysql conn"
    else:
        return "provide required params for mysql"

    if params.get("redshift"):
        if not params.get("redshift").get("schema").strip():
            return "provide redshift schema"
    else:
        return "provide required params for redshift"

    return None
