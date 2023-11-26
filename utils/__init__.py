import re
import sys
import json
import logging

from typing import Union, Tuple
from importlib import import_module
from confluent_kafka import Producer, Consumer
from confluent_kafka.serialization import MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient


####################
# Global Variables #
####################
SPLIT_REGEX = re.compile(".+'(.+?)'.+?([-]*\d+?)")


class Kafka:
    """
    Kafka Class: Set Kafka Producer/Consumer clients
    """

    def __init__(
        self,
        config: dict,
    ) -> None:
        self.config = config
        self._kafka_auth = self.config.get("kafka_auth", dict())
        self._kafka_producer_config = self.config.get("kafka_producer", dict())
        self._kafka_consumer_config = self.config.get("kafka_consumers", dict())
        self._kafka_consumer_qty = self._kafka_consumer_config.get("consumers", 0)
        self._kafka_consumer_config.pop("consumers", None)
        self._routing_rules_sink = self.config.get("routing_rules_sink", dict())

    def setConsumer(
        self,
        consumer_config: dict,
    ):
        """
        Set consumer client
        """
        try:
            consumer = Consumer(
                {
                    **self._kafka_auth,
                    **consumer_config,
                }
            )
            topics = list(self._routing_rules_sink.keys())
            if len(topics) > 0:
                consumer.subscribe(topics)
                logging.info(
                    f"Kafka Consumer {consumer_config['client.id']} subscribed to topic(s): {', '.join(topics)}"
                )
                return consumer
        except Exception:
            logging.error(sys_exc(sys.exc_info()))
            raise

    def setProducer(self):
        """
        Set producer client
        """
        try:
            self._kafka_producer = Producer(
                {
                    **self._kafka_auth,
                    **self._kafka_producer_config,
                }
            )
            self._kafka_producer.poll(0.0)
            return self._kafka_producer
        except Exception:
            logging.error(sys_exc(sys.exc_info()))
            raise

    def setSchemaRegistry(self):
        """
        Set schema registry client
        """
        self._schema_registry = SchemaRegistryClient(
            self.config.get("schema_registry", dict())
        )
        return self._schema_registry


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


def applyTransformation(
    smt: list,
    mqtt_topic: str,
    p_headers: dict,
    p_key: Union[str, dict],
    p_value: Union[str, dict],
) -> Tuple[Union[str, dict], Union[str, dict]]:
    """
    Apply transformations (as set on 'smt' on routing_rules_source)
    """
    result_headers = dict(p_headers)

    if isinstance(p_key, dict):
        result_key = dict(p_key)
    else:
        result_key = p_key

    if isinstance(p_value, dict):
        result_value = dict(p_value)
    else:
        result_value = p_value

    for t in smt:
        for cmd, arg in t.items():
            # set_value
            if cmd.startswith("set_value."):
                _, *field = cmd.split(".")
                field = ".".join(field)
                if str(arg).startswith("$"):
                    if str(arg).startswith("$topic.split("):
                        split_args = SPLIT_REGEX.findall(arg.replace('"', "'"))
                        if len(split_args) == 1:
                            split_by = split_args[0][0]
                            split_index = int(split_args[0][1])
                            if isinstance(result_value, dict):
                                try:
                                    result_value[field] = mqtt_topic.split(split_by)[
                                        split_index
                                    ]
                                except IndexError:
                                    logging.error(
                                        f"Invalid index on SMT argument: {arg} (comand {cmd})"
                                    )
                            else:
                                logging.error(
                                    f"Invalid SMT argument: {arg} (comand {cmd}). Value is not JSON"
                                )
                        else:
                            logging.error(f"Invalid SMT argument: {arg} (comand {cmd})")
                    elif str(arg) == "$topic":
                        if isinstance(result_value, dict):
                            result_value[field] = mqtt_topic
                        else:
                            logging.error(
                                f"Invalid SMT argument: {arg} (comand {cmd}). Value is not JSON"
                            )
                    else:
                        logging.error(f"Invalid SMT argument: {arg} (comand {cmd})")
                else:
                    if isinstance(result_value, dict):
                        result_value[field] = arg
                    else:
                        logging.error(
                            f"Invalid SMT argument: {arg} (comand {cmd}). Value is not JSON"
                        )

            # set_key
            elif cmd.startswith("set_key."):
                _, *field = cmd.split(".")
                field = ".".join(field)
                if str(arg).startswith("$"):
                    if str(arg).startswith("$topic.split("):
                        split_args = SPLIT_REGEX.findall(arg.replace('"', "'"))
                        if len(split_args) == 1:
                            split_by = split_args[0][0]
                            split_index = int(split_args[0][1])
                            try:
                                value = mqtt_topic.split(split_by)[split_index]
                            except IndexError:
                                logging.error(
                                    f"Invalid index on SMT argument: {arg} (comand {cmd})"
                                )
                            if isinstance(result_key, dict):
                                result_key[field] = value
                            else:
                                result_key = value
                        else:
                            logging.error(f"Invalid SMT argument: {arg} (comand {cmd})")
                    elif str(arg) == "$topic":
                        if isinstance(result_key, dict):
                            result_key[field] = mqtt_topic
                        else:
                            result_key = mqtt_topic
                    else:
                        logging.error(f"Invalid SMT argument: {arg} (comand {cmd})")
                else:
                    if isinstance(result_key, dict):
                        result_key[field] = arg
                    else:
                        result_key = arg

            # set_header
            elif cmd.startswith("set_header."):
                _, *field = cmd.split(".")
                field = ".".join(field)
                if str(arg).startswith("$"):
                    if str(arg).startswith("$topic.split("):
                        split_args = SPLIT_REGEX.findall(arg.replace('"', "'"))
                        if len(split_args) == 1:
                            split_by = split_args[0][0]
                            split_index = int(split_args[0][1])
                            try:
                                result_headers[field] = mqtt_topic.split(split_by)[
                                    split_index
                                ]
                            except IndexError:
                                logging.error(
                                    f"Invalid index on SMT argument: {arg} (comand {cmd})"
                                )
                        else:
                            logging.error(f"Invalid SMT argument: {arg} (comand {cmd})")
                    elif str(arg) == "$topic":
                        result_headers[field] = mqtt_topic
                    else:
                        logging.error(f"Invalid SMT argument: {arg} (comand {cmd})")
                else:
                    result_headers[field] = arg

            # rename_value
            elif cmd.startswith("rename_value."):
                _, *field = cmd.split(".")
                field = ".".join(field)
                if isinstance(result_value, dict):
                    result_value[arg] = result_value.get(field)
                    result_value.pop(field, None)
                else:
                    logging.error(
                        f"Invalid SMT argument: {arg} (comand {cmd}). Value is not JSON"
                    )

            # rename_key
            elif cmd.startswith("rename_key."):
                _, *field = cmd.split(".")
                field = ".".join(field)
                if isinstance(result_key, dict):
                    result_key[arg] = result_key.get(field)
                    result_key.pop(field, None)
                else:
                    logging.error(
                        f"Invalid SMT argument: {arg} (comand {cmd}). Key is not JSON"
                    )

            # rename_header
            elif cmd.startswith("rename_header."):
                _, *field = cmd.split(".")
                field = ".".join(field)
                result_headers[arg] = result_headers.get(field)
                result_headers.pop(field, None)

            # drop_value
            elif cmd == "drop_value":
                if isinstance(result_value, dict):
                    result_value.pop(arg, None)
                else:
                    logging.error(
                        f"Invalid SMT argument: {arg} (comand {cmd}). Value is not JSON"
                    )

            # drop_header
            elif cmd == "drop_header":
                result_headers.pop(arg, None)

            # drop_key
            elif cmd == "drop_key":
                if isinstance(result_key, dict):
                    result_key.pop(arg, None)
                else:
                    logging.error(
                        f"Invalid SMT argument: {arg} (comand {cmd}). Key is not JSON"
                    )

            # key_as_json
            elif cmd == "key_as_json":
                result_key = {
                    arg: result_key,
                }

            # value_to_key
            elif cmd.startswith("value_to_key."):
                _, *field = cmd.split(".")
                field = ".".join(field)
                if isinstance(result_key, dict):
                    if isinstance(result_value, dict):
                        result_key[field] = result_value.get(arg)
                        result_value.pop(arg, None)
                    else:
                        logging.error(
                            f"Invalid SMT argument: {arg} (comand {cmd}). Value is not JSON"
                        )
                else:
                    result_key = result_value.get(arg)

            else:
                logging.error(f"Invalid SMT command: {cmd} (argument {arg})")

    return (
        result_headers,
        result_key,
        result_value,
    )


def processMQTTMessage(
    self,
    obj,
    msg,
):
    """
    Process MQTT messages received
    """
    for data in self._routing_rules_source.values():
        encoding = data.get("encoding", "utf-8")
        if data["topic_regex"].match(msg.topic):
            try:
                # Set Kafka headers
                try:
                    kafka_headers = data["kafka_set_headers_class"].do(msg)
                except Exception:
                    kafka_headers = dict()

                # Deserialise MQTT topic
                mqtt_topic_deserialised = data[
                    "mq_deserialiser_topic_class"
                ].deserialise(
                    msg.topic,
                    encoding=encoding,
                )
                # Deserialise MQTT payload
                mqtt_payload_deserialised = data[
                    "mq_deserialiser_payload_class"
                ].deserialise(
                    msg.payload,
                    encoding=encoding,
                )

                # Apply transformations on the deserialised MQTT data
                smt = data.get("smt")
                if (
                    smt is not None
                    and isinstance(mqtt_payload_deserialised, dict)
                    and isinstance(smt, list)
                ):
                    (
                        kafka_headers,
                        mqtt_topic_deserialised,
                        mqtt_payload_deserialised,
                    ) = applyTransformation(
                        smt,
                        msg.topic,
                        kafka_headers,
                        mqtt_topic_deserialised,
                        mqtt_payload_deserialised,
                    )

                # Serialise Kafka key
                kafka_key_serialised = data["kafka_serialiser_key_class"].serialise(
                    mqtt_topic_deserialised,
                    encoding=encoding,
                    schema_registry=self._schema_registry,
                    schema_str=data["kafka_serialiser_key_schema"],
                    kafka_topic=data["kafka_topic"],
                    message_field=MessageField.KEY,
                )

                # Serialise kafka value
                kafka_value_serialised = data["kafka_serialiser_value_class"].serialise(
                    mqtt_payload_deserialised,
                    encoding=encoding,
                    schema_registry=self._schema_registry,
                    schema_str=data["kafka_serialiser_value_schema"],
                    kafka_topic=data["kafka_topic"],
                    message_field=MessageField.VALUE,
                )

                # Publish message to Kafka
                self._kafka_producer.produce(
                    headers=list(kafka_headers.items()),
                    topic=data["kafka_topic"],
                    key=kafka_key_serialised,
                    value=kafka_value_serialised,
                )

            except Exception:
                err = sys_exc(sys.exc_info())
                logging.error(err)

                # Publish message to Kafka (DLQ)
                kafka_headers["error"] = err
                self._kafka_producer.produce(
                    headers=list(kafka_headers.items()),
                    topic=data["kafka_topic_dlq"],
                    key=msg.topic.encode(encoding)
                    if not isinstance(msg.topic, bytes)
                    else msg.topic,
                    value=msg.payload.encode(encoding)
                    if not isinstance(msg.payload, bytes)
                    else msg.payload,
                )

            if data.get("stop_processing_if_matched", False):
                break
