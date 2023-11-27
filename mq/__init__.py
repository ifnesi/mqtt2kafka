import re
import sys
import json
import time
import yaml
import queue
import signal
import logging
import threading

from typing import Union, Tuple, Any
from importlib import import_module
from confluent_kafka import Producer, Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import SerializationContext, MessageField

from utils import sys_exc, getSerdesSchema


class SourceBase:
    def __init__(
        self,
        config_yaml: str,
        regex_pattern_lambda: Any,
    ) -> None:
        """
        Make sure to overwrite self._source_disconnect_method with the method to close the connection with the source system
        """
        # Set signal handlers
        self._source_disconnect_method = lambda: logging.warning(
            "Unable to disconnect the Source system. No '_source_disconnect_method' passed!"
        )  # This need to be overwritten by the child inheriting SourceBase with the method to close the connection to the Source system
        self._handler_called = False
        self._handler_can_exit = True
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        # System queue (from Kafka to Sink system)
        self._kafka_to_sink_queue = queue.Queue()

        # Get config file (YAML)
        with open(config_yaml, "r") as f:
            self.config = yaml.safe_load(f)

        # Kafka client
        self._kafka = Kafka(self.config)
        self._kafka_producer = self._kafka.setProducer()

        # Schema Registry client
        self._schema_registry = self._kafka.setSchemaRegistry()

        # Get routing rules (data received from Source to Kafka: Source connection)
        self._routing_rules_source = self.config.get(
            "routing_rules_source",
            dict(),
        )
        for topic in self._routing_rules_source.keys():
            # Source Topic regex
            self._routing_rules_source[topic]["topic_regex"] = re.compile(
                regex_pattern_lambda(topic)
            )
            # Source topic deserialiser
            (
                self._routing_rules_source[topic]["mq_deserialiser_topic_class"],
                self._routing_rules_source[topic]["mq_deserialiser_topic_schema"],
            ) = getSerdesSchema(
                self._routing_rules_source[topic],
                "mq_deserialiser_topic",
            )
            # Source payload deserialiser
            (
                self._routing_rules_source[topic]["mq_deserialiser_payload_class"],
                self._routing_rules_source[topic]["mq_deserialiser_payload_schema"],
            ) = getSerdesSchema(
                self._routing_rules_source[topic],
                "mq_deserialiser_payload",
            )
            # Kafka set headers
            self._routing_rules_source[topic][
                "kafka_set_headers_class"
            ] = import_module(
                self._routing_rules_source[topic].get("kafka_set_headers", "tools")
            ).Tools()
            # Kafka key serialiser
            (
                self._routing_rules_source[topic]["kafka_serialiser_key_class"],
                self._routing_rules_source[topic]["kafka_serialiser_key_schema"],
            ) = getSerdesSchema(
                self._routing_rules_source[topic],
                "kafka_serialiser_key",
            )
            # Kafka value serialiser
            (
                self._routing_rules_source[topic]["kafka_serialiser_value_class"],
                self._routing_rules_source[topic]["kafka_serialiser_value_schema"],
            ) = getSerdesSchema(
                self._routing_rules_source[topic],
                "kafka_serialiser_value",
            )

        # Get Kafka routing rules (data received from Kafka to Sink system)
        for topic in self._kafka._routing_rules_sink.keys():
            # Kafka key deserialiser
            (
                self._kafka._routing_rules_sink[topic]["kafka_deserialiser_key_class"],
                _,
            ) = getSerdesSchema(
                self._kafka._routing_rules_sink[topic],
                "kafka_deserialiser_key",
            )
            # Value key deserialiser
            (
                self._kafka._routing_rules_sink[topic][
                    "kafka_deserialiser_value_class"
                ],
                _,
            ) = getSerdesSchema(
                self._kafka._routing_rules_sink[topic],
                "kafka_deserialiser_value",
            )
            # Sink payload serialiser
            (
                self._kafka._routing_rules_sink[topic]["mq_serialiser_payload_class"],
                _,
            ) = getSerdesSchema(
                self._kafka._routing_rules_sink[topic],
                "mq_serialiser_payload",
            )

    def _signal_handler(
        self,
        sig,
        frame,
    ) -> None:
        """
        Signal Handler for Gracious Shutdown
        """
        self._handler_called = True
        if self._handler_can_exit:
            try:
                logging.info("Disconnecting Source client")
                self._source_disconnect_method()
            except Exception:
                logging.error(sys_exc(sys.exc_info()))

            try:
                logging.info("Flushing Kafka producer")
                self._kafka_producer.flush()
            except Exception:
                logging.error(sys_exc(sys.exc_info()))

            logging.info(f"Closing threads...")
            self._consumer_threads_stop = True
            for thread in self._consumer_threads:
                try:
                    thread.join()
                except Exception:
                    logging.error(sys_exc(sys.exc_info()))

            for n, consumer in enumerate(self._consumers):
                try:
                    logging.info(f"Closing Kafka consumer: {self._kafka_group_id}-{n}")
                    consumer.close()
                except Exception:
                    logging.error(sys_exc(sys.exc_info()))

            sys.exit(0)

    def _start_kafka_consumer_threads(
        self,
        publish_method: Any,
    ) -> None:
        self._kafka_group_id = self._kafka._kafka_consumer_config["group.id"]
        self._consumers = list()
        self._consumer_threads = list()
        self._consumer_threads_stop = False
        if (
            isinstance(self._kafka._kafka_consumer_qty, int)
            and self._kafka._kafka_consumer_qty > 0
        ):
            for n in range(self._kafka._kafka_consumer_qty):
                consumer_config = {
                    **self._kafka._kafka_consumer_config,
                    "client.id": f"{self._kafka_group_id}-{n}",
                }
                # Start kafka consumer within consumer group
                consumer = self._kafka.setConsumer(consumer_config)
                if consumer is not None:
                    self._consumers.append(consumer)
                    self._consumer_threads.append(
                        threading.Thread(
                            target=self._kafka_consumer_loop,
                            args=[
                                n,
                                self._consumers[n],
                                lambda: self._consumer_threads_stop,
                            ],
                        )
                    )

            # Start queue thread
            self._consumer_threads.append(
                threading.Thread(
                    target=self._read_kafka_to_sink_queue,
                    args=[
                        lambda: self._consumer_threads_stop,
                        publish_method,
                    ],
                )
            )

            # Start all threads
            for thread in self._consumer_threads:
                thread.start()

    def _read_kafka_to_sink_queue(
        self,
        stop: bool,
        publish_method: Any,
    ) -> None:
        """
        Queue thread: Data to be sent to MQTT
        """
        while True:
            if stop():
                logging.info(f"> Closing Queue thread")
                break

            try:
                item = self._kafka_to_sink_queue.get(
                    block=True,
                    timeout=0.1,
                )
                if item is not None:
                    topic, payload, qos = item
                    publish_method(
                        topic,
                        payload,
                        qos=qos,
                    )
            except queue.Empty:
                pass

    def _kafka_consumer_loop(
        self,
        n: int,
        consumer,
        stop: bool,
    ) -> None:  # TBD (ADJUST THIS PART)
        """
        Thread running each Kafka consumer (single consumer group)
        """
        next_attempt = 0
        while True:
            if stop():
                logging.info(f"> Closing Kafka Consumer thread: {n}")
                break

            if next_attempt <= time.time():
                try:
                    msg = consumer.poll(timeout=0.1)
                    if msg is not None:
                        if msg.error():
                            logging.error(msg.error())
                        else:
                            data = self._kafka._routing_rules_sink.get(
                                msg.topic(),
                                dict(),
                            )
                            encoding = data.get("encoding", "utf-8")
                            mq_topic = data.get("mq_topic")
                            mq_payload = data.get("mq_payload")
                            mq_qos = data.get("mq_qos", 0)
                            if "$header." in mq_topic or "$header." in mq_payload:
                                headers = (
                                    dict()
                                    if msg.headers() is None
                                    else {
                                        k: v.decode(encoding) for k, v in msg.headers()
                                    }
                                )
                            else:
                                None

                            if "$key" in mq_topic or "$key" in mq_payload:
                                kafka_key_deserialised = data[
                                    "kafka_deserialiser_key_class"
                                ].deserialise(
                                    msg.key(),
                                    encoding=encoding,
                                    schema_registry=self._schema_registry,
                                    kafka_topic=msg.topic(),
                                    message_field=MessageField.KEY,
                                )
                            else:
                                kafka_key_deserialised = None

                            if "$value" in mq_topic or "$value" in mq_payload:
                                kafka_value_deserialised = data[
                                    "kafka_deserialiser_value_class"
                                ].deserialise(
                                    msg.value(),
                                    encoding=encoding,
                                    schema_registry=self._schema_registry,
                                    kafka_topic=msg.topic(),
                                    message_field=MessageField.VALUE,
                                )
                            else:
                                kafka_value_deserialised = None

                            # Set MQ Topic
                            topic = list()
                            for arg in mq_topic.split("+"):
                                if arg == "$key":
                                    topic.append(kafka_key_deserialised)
                                elif arg == "$value":
                                    topic.append(kafka_value_deserialised)
                                elif arg.startswith("$value."):
                                    _, *field = arg.split(".")
                                    field = ".".join(field)
                                    if isinstance(kafka_value_deserialised, dict):
                                        topic.append(
                                            kafka_value_deserialised.get(field, "")
                                        )
                                    else:
                                        logging.error(
                                            f"Invalid mq_topic argument: {arg} on {mq_topic} (Topic {msg.topic()}). Value is not JSON"
                                        )
                                elif arg.startswith("$header."):
                                    _, *field = arg.split(".")
                                    field = ".".join(field)
                                    topic.append(headers.get(field))
                                else:
                                    topic.append(arg)

                            # Set MQ Payload
                            if mq_payload == "$key":
                                payload = kafka_key_deserialised
                            elif mq_payload == "$value":
                                payload = kafka_value_deserialised
                            elif mq_payload.startswith("$value."):
                                _, *field = mq_payload.split(".")
                                field = ".".join(field)
                                if isinstance(kafka_value_deserialised, dict):
                                    payload = kafka_value_deserialised.get(field)
                                else:
                                    payload = None
                                    logging.error(
                                        f"Invalid mq_payload argument: {mq_payload} (Topic {msg.topic()}). Value is not JSON"
                                    )
                            elif mq_payload.startswith("$header."):
                                _, *field = mq_payload.split(".")
                                field = ".".join(field)
                                payload = headers.get(field)
                            else:
                                payload = None

                            # Serialise payload
                            mq_payload_serialised = data[
                                "mq_serialiser_payload_class"
                            ].serialise(
                                payload,
                                encoding=encoding,
                            )

                            # Add to queue data to be published to Sink system
                            self._kafka_to_sink_queue.put(
                                [
                                    "".join([f"{n}" for n in topic]),
                                    mq_payload_serialised,
                                    mq_qos,
                                ],
                                block=True,
                            )
                except Exception:
                    logging.error(sys_exc(sys.exc_info()))
                    next_attempt = time.time() + 3

    def _process_source_message(
        self,
        msg,
    ):
        """
        Process messages received from the Source system
        """
        self._handler_can_exit = False

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
                    kafka_value_serialised = data[
                        "kafka_serialiser_value_class"
                    ].serialise(
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

        self._handler_can_exit = True
        if self._handler_called:
            self._signal_handler(None, None)


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
    SPLIT_REGEX = re.compile(".+'(.+?)'.+?([-]*\d+?)")
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
