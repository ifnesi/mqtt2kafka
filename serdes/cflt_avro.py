from typing import Union, Any

from serdes import SerdesBase
from confluent_kafka.serialization import SerializationContext
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer


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
        client = AvroDeserializer(
            schema_registry,
        )
        return client(
            value,
            SerializationContext(
                kafka_topic,
                message_field,
            ),
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
        client = AvroSerializer(
            schema_registry,
            schema_str,
        )
        return client(
            value,
            SerializationContext(
                kafka_topic,
                message_field,
            ),
        )
