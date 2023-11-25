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

import re
import sys
import json
import logging

from typing import Union, Tuple
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
)

from utils import sys_exc


####################
# Global Variables #
####################
TOPIC_SPLIT_REGEX = re.compile(".+'(.+?)'.+?([-]*\d+?)")


class Parsers:
    def __init__(self) -> None:
        pass

    def setHeaders(self, msg) -> dict:
        return {
            "mqtt.message.id": str(msg.mid),
            "mqtt.qos": str(msg.qos),
            "mqtt.retained": "true" if msg.retain > 0 else "false",
            "mqtt.duplicate": "true" if msg.dup > 0 else "false",
        }

    def parse(
        self,
        obj,
        msg,
        encoding: str = "utf-8",
    ) -> tuple():
        parsed_headers = self.setHeaders(msg)
        parsed_topic = msg.topic
        parsed_payload = msg.payload
        return (
            parsed_headers,
            parsed_topic,
            parsed_payload,
        )


class Parser(Parsers):
    def __init__(self):
        super().__init__()


def jsonify(
    value: str,
    encoding: str = "utf-8",
):
    try:
        value_decoded = value.decode(encoding) if isinstance(value, bytes) else value
        if isinstance(value_decoded, str):
            value_json = json.loads(value_decoded)
            if isinstance(value_json, dict):
                return value_json
    except:
        pass

    return value_decoded


def applyTransformation(
    smt: list,
    mqtt_topic: str,
    p_headers: dict,
    p_key: Union[str, dict],
    p_value: Union[str, dict],
) -> Tuple[Union[str, dict], Union[str, dict]]:
    """
    Apply transformations (as set on 'smt' on mq_routing_rules)
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
            # setValue
            if cmd.startswith("setValue."):
                _, *field = cmd.split(".")
                field = ".".join(field)
                if str(arg).startswith("$"):
                    if str(arg).startswith("$topic.split("):
                        split_args = TOPIC_SPLIT_REGEX.findall(arg.replace('"', "'"))
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

            # setKey
            elif cmd.startswith("setKey."):
                _, *field = cmd.split(".")
                field = ".".join(field)
                if str(arg).startswith("$"):
                    if str(arg).startswith("$topic.split("):
                        split_args = TOPIC_SPLIT_REGEX.findall(arg.replace('"', "'"))
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

            # setHeader
            elif cmd.startswith("setHeader."):
                _, *field = cmd.split(".")
                field = ".".join(field)
                if str(arg).startswith("$"):
                    if str(arg).startswith("$topic.split("):
                        split_args = TOPIC_SPLIT_REGEX.findall(arg.replace('"', "'"))
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

            # renameValue
            elif cmd.startswith("renameValue."):
                _, *field = cmd.split(".")
                field = ".".join(field)
                if isinstance(result_value, dict):
                    result_value[arg] = result_value.get(field)
                    result_value.pop(field, None)
                else:
                    logging.error(
                        f"Invalid SMT argument: {arg} (comand {cmd}). Value is not JSON"
                    )

            # renameKey
            elif cmd.startswith("renameKey."):
                _, *field = cmd.split(".")
                field = ".".join(field)
                if isinstance(result_key, dict):
                    result_key[arg] = result_key.get(field)
                    result_key.pop(field, None)
                else:
                    logging.error(
                        f"Invalid SMT argument: {arg} (comand {cmd}). Key is not JSON"
                    )

            # renameHeader
            elif cmd.startswith("renameHeader."):
                _, *field = cmd.split(".")
                field = ".".join(field)
                result_headers[arg] = result_headers.get(field)
                result_headers.pop(field, None)

            # dropValue
            elif cmd == "dropValue":
                if isinstance(result_value, dict):
                    result_value.pop(arg, None)
                else:
                    logging.error(
                        f"Invalid SMT argument: {arg} (comand {cmd}). Value is not JSON"
                    )

            # dropHeader
            elif cmd == "dropHeader":
                result_headers.pop(arg, None)

            # dropKey
            elif cmd == "dropKey":
                if isinstance(result_key, dict):
                    result_key.pop(arg, None)
                else:
                    logging.error(
                        f"Invalid SMT argument: {arg} (comand {cmd}). Key is not JSON"
                    )

            # keyAsJSON
            elif cmd == "keyAsJSON":
                result_key = {
                    arg: result_key,
                }

            # valueToKey
            elif cmd.startswith("valueToKey."):
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


def processMessage(
    self,
    mqtt,
    obj,
    msg,
):
    """
    Process MQTT messages received
    """
    for topic, data in self._mq_routing_rules.items():
        encoding = data.get("encoding", "utf-8")
        if data["topic_regex"].match(msg.topic):
            try:
                # Parser message from MQTT broker
                (
                    parsed_headers,
                    parsed_key,
                    parsed_value,
                ) = self._mq_routing_rules[
                    topic
                ]["mod"].parse(
                    obj,
                    msg,
                    encoding=encoding,
                )

                # Apply transformations on value (before serialisation)
                smt = data.get("smt")
                if (
                    smt is not None
                    and isinstance(parsed_value, dict)
                    and isinstance(smt, list)
                ):
                    parsed_headers, parsed_key, parsed_value = applyTransformation(
                        smt,
                        msg.topic,
                        parsed_headers,
                        parsed_key,
                        parsed_value,
                    )

                # Serialise key
                if data["kafka_schema_key_client"] is None:
                    if isinstance(parsed_key, dict):
                        msg_key = json.dumps(parsed_key).encode(encoding)
                    elif not isinstance(parsed_key, bytes):
                        msg_key = str(parsed_key).encode(encoding)
                    else:
                        msg_key = parsed_key
                else:
                    msg_key = data["kafka_schema_key_client"](
                        parsed_key,
                        SerializationContext(
                            data["kafka_topic"],
                            MessageField.KEY,
                        ),
                    )

                # Serialise value
                if data["kafka_schema_value_client"] is None:
                    if isinstance(parsed_value, dict):
                        msg_value = json.dumps(parsed_value).encode(encoding)
                    elif not isinstance(parsed_value, bytes):
                        msg_value = str(parsed_value).encode(encoding)
                    else:
                        msg_value = parsed_value
                else:
                    msg_value = data["kafka_schema_value_client"](
                        parsed_value,
                        SerializationContext(
                            data["kafka_topic"],
                            MessageField.VALUE,
                        ),
                    )

                # Publish message to Kafka
                self._kafka_producer.produce(
                    headers=list(parsed_headers.items()),
                    topic=data["kafka_topic"],
                    key=msg_key,
                    value=msg_value,
                )

            except Exception:
                err = sys_exc(sys.exc_info())
                logging.error(err)

                # Publish message to Kafka (DLQ)
                parsed_headers["error"] = err
                self._kafka_producer.produce(
                    headers=list(parsed_headers.items()),
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
