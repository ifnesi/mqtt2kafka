import re
import ssl
import sys
import json
import time
import yaml
import queue
import signal
import logging
import threading

import paho.mqtt.client as mqtt

from importlib import import_module
from confluent_kafka.serialization import SerializationContext, MessageField

from utils import Kafka, sys_exc, getSerdesSchema, processMQTTMessage


class MQTT:
    """
    MQTT Class: Set MQTT Producer/Consumer clients and start all threads
    Main class to be called at start, all threads will be spawn up here
    """

    PROTOCOLS = {
        "MQTTv31": 3,
        "MQTTv311": 4,
        "MQTTv5": 5,
    }

    def __init__(
        self,
        config_yaml: str,
        log: bool = False,
    ) -> None:
        # Set signal handlers
        self._handler_called = False
        self._handler_can_exit = True
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        # System queue (from Kafka to MQTT: Sink connection)
        self._queue = queue.Queue()

        # Get config file (YAML)
        with open(config_yaml, "r") as f:
            self.config = yaml.safe_load(f)

        # Kafka client
        kafka = Kafka(self.config)
        self._kafka_producer = kafka.setProducer()

        # Schema Registry client
        self._schema_registry = kafka.setSchemaRegistry()

        # MQTT client
        self._mqtt_client = mqtt.Client(
            client_id=self.config.get("mqtt", dict()).get("client_id"),
            clean_session=self.config.get("mqtt", dict()).get("clean_session"),
            transport=self.config.get("mqtt", dict()).get("transport", "tcp"),
            reconnect_on_failure=self.config.get("mqtt", dict()).get(
                "reconnect_on_failure", True
            ),
            protocol=MQTT.PROTOCOLS.get(
                self.config.get("mqtt", dict()).get("protocol", "MQTTv311"),
                MQTT.PROTOCOLS["MQTTv311"],
            ),
        )

        tls_version = self.config.get("mqtt", dict()).get("tls_version")
        if tls_version is not None:
            self._mqtt_client.tls_set(
                ca_certs=self.config.get("mqtt", dict()).get("ca_certs"),
                certfile=self.config.get("mqtt", dict()).get("certfile"),
                keyfile=self.config.get("mqtt", dict()).get("keyfile"),
                cert_reqs=ssl.__dict__.get(
                    self.config.get("mqtt", dict()).get("cert_reqs")
                ),
                tls_version=ssl.__dict__.get(tls_version),
            )

        self._mqtt_client.on_connect = self._on_mqtt_connect
        self._mqtt_client.on_message = self._on_mqtt_message
        if log:
            self._mqtt_client.on_log = self._on_mqtt_log

        mqtt_username = self.config.get("mqtt", dict()).get("username")
        mqtt_password = self.config.get("mqtt", dict()).get("password")
        if mqtt_username is not None and mqtt_password is not None:
            self._mqtt_client.username_pw_set(
                mqtt_username,
                mqtt_password,
            )

        # Get MQ routing rules (data received from MQTT to Kafka: Source connection)
        self._routing_rules_source = self.config.get(
            "routing_rules_source",
            dict(),
        )
        for topic in self._routing_rules_source.keys():
            # MQTT Topic regex
            self._routing_rules_source[topic]["topic_regex"] = re.compile(
                topic.replace("#", ".*").replace("+/", ".*?/").replace("+", ".*")
            )
            # MQTT topic deserialiser
            (
                self._routing_rules_source[topic]["mq_deserialiser_topic_class"],
                self._routing_rules_source[topic]["mq_deserialiser_topic_schema"],
            ) = getSerdesSchema(
                self._routing_rules_source[topic],
                "mq_deserialiser_topic",
            )
            # MQTT payload deserialiser
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

        # Get Kafka routing rules (data received from Kafka to MQTT: Sink connection)
        for topic in kafka._routing_rules_sink.keys():
            # Kafka key deserialiser
            (
                kafka._routing_rules_sink[topic]["kafka_deserialiser_key_class"],
                _,
            ) = getSerdesSchema(
                kafka._routing_rules_sink[topic],
                "kafka_deserialiser_key",
            )
            # Value key deserialiser
            (
                kafka._routing_rules_sink[topic]["kkafka_deserialiser_value_class"],
                _,
            ) = getSerdesSchema(
                kafka._routing_rules_sink[topic],
                "kafka_deserialiser_value",
            )
            # MQTT topic serialiser
            (
                kafka._routing_rules_sink[topic]["mq_serialiser_topic_class"],
                _,
            ) = getSerdesSchema(
                kafka._routing_rules_sink[topic],
                "mq_serialiser_topic",
            )
            # MQTT payload serialiser
            (
                kafka._routing_rules_sink[topic]["mq_serialiser_payload_class"],
                _,
            ) = getSerdesSchema(
                kafka._routing_rules_sink[topic],
                "mq_serialiser_payload",
            )

        # Connect to MQTT broker (subscription to topics is on_connect)
        self._mqtt_connect()

        # Start Kafka consumer(s) as separate threads
        self._kafka_group_id = kafka._kafka_consumer_config["group.id"]
        self._consumers = list()
        self._consumer_threads = list()
        self._consumer_threads_stop = False
        if isinstance(kafka._kafka_consumer_qty, int) and kafka._kafka_consumer_qty > 0:
            for n in range(kafka._kafka_consumer_qty):
                consumer_config = {
                    **kafka._kafka_consumer_config,
                    "client.id": f"{self._kafka_group_id}-{n}",
                }
                # Start kafka consumer within consumer group
                consumer = kafka.setConsumer(consumer_config)
                if consumer is not None:
                    self._consumers.append(consumer)
                    self._consumer_threads.append(
                        threading.Thread(
                            target=self._kafka_consumer_loop,
                            args=[
                                n,
                                self._consumers[n],
                                kafka,
                                lambda: self._consumer_threads_stop,
                            ],
                        )
                    )

            # Start queue thread
            self._consumer_threads.append(
                threading.Thread(
                    target=self._read_queue,
                    args=[
                        lambda: self._consumer_threads_stop,
                    ],
                )
            )

            # Start all threads
            for thread in self._consumer_threads:
                thread.start()

        # MQTT client (loop running in main thread)
        self._mqtt_client.loop_forever()

    def _kafka_consumer_loop(
        self,
        n: int,
        consumer,
        kafka: Kafka,
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
                            topic_routing_rules = kafka._routing_rules_sink.get(
                                msg.topic(),
                                dict(),
                            )
                            encoding = topic_routing_rules.get("encoding", "utf-8")
                            mq_topic = topic_routing_rules.get("mq_topic")
                            mq_payload = topic_routing_rules.get("mq_payload")
                            if mq_topic.startswith("header.") or mq_payload.startswith(
                                "header."
                            ):
                                headers = (
                                    dict()
                                    if msg.headers() is None
                                    else {
                                        k: v.decode(encoding) for k, v in msg.headers()
                                    }
                                )
                            else:
                                None

                            if mq_topic == "key" or mq_payload == "key":
                                key = (
                                    msg.key().decode(encoding)
                                    if topic_routing_rules["kafka_schema_key_client"]
                                    is None
                                    else topic_routing_rules["kafka_schema_key_client"](
                                        msg.key(),
                                        SerializationContext(
                                            msg.topic(),
                                            MessageField.KEY,
                                        ),
                                    )
                                )
                            else:
                                key = None

                            if (
                                mq_topic == "value"
                                or mq_payload == "value"
                                or mq_topic.startswith("value.")
                                or mq_payload.startswith("value.")
                            ):
                                value = (
                                    msg.value().decode(encoding)
                                    if topic_routing_rules["kafka_schema_value_client"]
                                    is None
                                    else topic_routing_rules[
                                        "kafka_schema_value_client"
                                    ](
                                        msg.value(),
                                        SerializationContext(
                                            msg.topic(),
                                            MessageField.VALUE,
                                        ),
                                    )
                                )
                            else:
                                value = None

                            # Set MQ Topic
                            if mq_topic == "key":
                                topic = key
                            elif mq_topic == "value":
                                topic = value
                            elif mq_topic.startswith("value."):
                                _, *field = mq_topic.split(".")
                                field = ".".join(field)
                                if isinstance(value, dict):
                                    topic = value.get(field)
                                else:
                                    topic = None
                                    logging.error(
                                        f"Invalid mq_topic argument: {mq_topic} (Topic {msg.topic()}). Value is not JSON"
                                    )
                            elif mq_topic.startswith("header."):
                                _, *field = mq_topic.split(".")
                                field = ".".join(field)
                                topic = headers.get(field)
                            else:
                                topic = None

                            # Set MQ Payload
                            if mq_payload == "key":
                                payload = key
                            elif mq_payload == "value":
                                payload = value
                            elif mq_payload.startswith("value."):
                                _, *field = mq_payload.split(".")
                                field = ".".join(field)
                                if isinstance(value, dict):
                                    payload = value.get(field)
                                else:
                                    payload = None
                                    logging.error(
                                        f"Invalid mq_payload argument: {mq_payload} (Topic {msg.topic()}). Value is not JSON"
                                    )
                            elif mq_payload.startswith("header."):
                                _, *field = mq_payload.split(".")
                                field = ".".join(field)
                                payload = headers.get(field)
                            else:
                                payload = None

                            # Add to queue data to be published to MQTT
                            self._queue.put(
                                [
                                    topic,
                                    json.dumps(
                                        payload,
                                        default=str,
                                    )
                                    if isinstance(payload, dict)
                                    else payload,
                                    topic_routing_rules.get("mq_qos", 0),
                                ],
                                block=True,
                            )
                except Exception:
                    logging.error(sys_exc(sys.exc_info()))
                    next_attempt = time.time() + 3

    def _read_queue(
        self,
        stop: bool,
    ) -> None:
        """
        Queue thread: Data to be sent to MQTT
        """
        while True:
            if stop():
                logging.info(f"> Closing Queue thread")
                break

            try:
                item = self._queue.get(
                    block=True,
                    timeout=0.1,
                )
                if item is not None:
                    topic, payload, qos = item
                    self._mqtt_publish(
                        topic,
                        payload,
                        qos=qos,
                    )
            except queue.Empty:
                pass

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
                logging.info("Disconnecting MQTT client")
                self._mqtt_client.disconnect()
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

    def _on_mqtt_log(
        self,
        mqtt,
        obj,
        level,
        string,
    ) -> None:
        logging.info(f"{level} - {string}")

    def _on_mqtt_connect(
        self,
        mqtt,
        userdata,
        flags,
        rc,
    ) -> None:
        self._mqtt_subscribe()

    def _mqtt_connect(
        self,
    ) -> bool:
        """
        Connect to MQTT broker
        """
        try:
            return self._mqtt_client.connect(
                self.config.get("mqtt", dict()).get("host"),
                self.config.get("mqtt", dict()).get("port"),
                keepalive=self.config.get("mqtt", dict()).get("keepalive"),
            )
        except Exception:
            logging.error(sys_exc(sys.exc_info()))
            raise

    def _mqtt_subscribe(
        self,
    ) -> None:
        """
        Subscribe to MQTT topics (automatically set on connect)
        """
        if not self._mqtt_client.is_connected():
            self._mqtt_connect()

        for topic, params in self._routing_rules_source.items():
            qos = params.get("qos", 0)
            self._mqtt_client.subscribe(topic, qos)
            transform_class_reference = params.get("parser")
            if transform_class_reference is not None:
                self._routing_rules_source[topic]["mod"] = import_module(
                    transform_class_reference
                ).Parser()

    def _mqtt_publish(
        self,
        topic: str,
        payload,
        qos: int = 0,
    ) -> None:
        """
        Publish to MQTT broker
        """
        if not self._mqtt_client.is_connected():
            self._mqtt_connect()

        try:
            self._mqtt_client.publish(
                topic,
                payload,
                qos=qos,
            )
        except Exception:
            logging.error(sys_exc(sys.exc_info()))
            raise

    def _on_mqtt_message(
        self,
        mqtt,
        obj,
        msg,
    ) -> None:
        self._handler_can_exit = False
        processMQTTMessage(
            self,
            obj,
            msg,
        )
        self._handler_can_exit = True
        if self._handler_called:
            self._signal_handler(None, None)


if __name__ == "__main__":
    # Logging config
    logging.basicConfig(
        format="%(asctime)s.%(msecs)03d [%(levelname)s]: %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    MQTT("config/example.yaml", log=True)
