from typing import Union, Any

from serdes import SerdesBase, jsonify


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
        return jsonify(
            super().deserialise(
                value,
                encoding=encoding,
            )
        )
