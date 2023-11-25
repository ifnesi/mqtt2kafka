# -*- coding: utf-8 -*-
#
# Copyright 2022 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
import logging

from confluent_kafka import Producer, Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from confluent_kafka.schema_registry.json_schema import JSONSerializer, JSONDeserializer
from confluent_kafka.schema_registry.protobuf import (
    ProtobufSerializer,
    ProtobufDeserializer,
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
        self._kafka_routing_rules = self.config.get("kafka_routing_rules", dict())

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
            topics = list(self._kafka_routing_rules.keys())
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

    def setSerialiser(
        self,
        schema_type: str,
        schema_file: str,
    ):
        """
        Set serialiser class
        """
        if schema_type == "avro":
            return AvroSerializer(
                self._schema_registry,
                open(schema_file, "r").read(),
            )
        elif schema_type == "json":
            return JSONSerializer(
                open(schema_file, "r").read(),
                self._schema_registry,
            )
        elif schema_type == "protobuf":
            return ProtobufSerializer(
                self._schema_registry,
                open(schema_file, "r").read(),
                {
                    "use.deprecated.format": False,
                },
            )
        else:
            return None

    def setDeserialiser(
        self,
        schema_type: str,
    ):
        """
        Set deserialiser class
        """
        if schema_type == "avro":
            return AvroDeserializer(self._schema_registry)
        elif schema_type == "json":
            return JSONDeserializer(self._schema_registry)
        elif schema_type == "protobuf":
            return ProtobufDeserializer(self._schema_registry)
        else:
            return None


def sys_exc(err) -> str:
    """
    Get details about Exceptions
    """
    exc_type, exc_obj, exc_tb = err
    return f"{exc_type} | {exc_tb.tb_lineno} | {exc_obj}"
