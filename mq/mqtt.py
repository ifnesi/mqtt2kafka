import ssl
import sys
import queue
import logging

import paho.mqtt.client as mqtt

from importlib import import_module

from mq import SourceBase
from utils import sys_exc


class MQTT(SourceBase):
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
    ):
        super().__init__(
            config_yaml=config_yaml,
            regex_pattern_lambda=(
                lambda topic: topic.replace("#", ".*")
                .replace("+/", ".*?/")
                .replace("+", ".*")
            ),
        )

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

        # This part here is needed for the signal handler
        self._source_disconnect_method = self._mqtt_client.disconnect

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

        # Start Kafka consumer(s) as separate threads
        self._start_kafka_consumer_threads(self._mqtt_publish)

        # Connect to MQTT broker (subscription to topics is on_connect)
        self._mqtt_connect()

        # MQTT client (loop running in main thread)
        self._mqtt_client.loop_forever()

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
        self._process_source_message(msg)
