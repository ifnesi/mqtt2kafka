import xmltodict

from typing import Union, Any

from serdes import SerdesBase


class Serdes(SerdesBase):
    def __init__(self):
        super().__init__()

    def deserialise(
        self,
        value: Union[str, bytes],
        encoding: str = "utf-8",
        schema_registry: Any = None,
        kafka_topic: Any = None,
        message_field: Any = None,
    ) -> Union[dict, str]:
        """
        XML will be deserialised as JSON
        """
        return xmltodict.parse(
            super().deserialise(
                value,
                encoding=encoding,
            ),
            encoding=encoding,
        )

    def serialise(
        self,
        value: Union[dict, str, int, float],
        encoding: str = "utf-8",
        schema_registry: Any = None,
        schema_str: str = None,
        kafka_topic: Any = None,
        message_field: Any = None,
    ) -> bytes:
        if isinstance(value, dict):
            new_value = xmltodict.unparse(
                value,
                encoding=encoding,
            )
        else:
            new_value = value

        return super().serialise(
            new_value,
            encoding=encoding,
        )
