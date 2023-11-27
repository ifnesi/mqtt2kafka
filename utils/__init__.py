from importlib import import_module


def sys_exc(err) -> str:
    """
    Get details about Exceptions
    """
    exc_type, exc_obj, exc_tb = err
    return f"{exc_type} | {exc_tb.tb_lineno} | {exc_obj}"


def getSerdesSchema(
    config: dict,
    key: str,
) -> tuple:
    class_name, *schema_file_name = config.get(
        key,
        "serdes.std_string",
    ).split("|")
    class_name = class_name.strip()
    schema_file_name = "|".join(schema_file_name).strip()
    return (
        import_module(class_name).Serdes(),
        open(schema_file_name, "r").read() if schema_file_name else None,
    )
