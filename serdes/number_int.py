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
        return int(
            super().deserialise(
                value,
                encoding=encoding,
            )
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
        return super().serialise(
            str(value),
            encoding=encoding,
        )
