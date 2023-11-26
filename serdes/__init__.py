import sys
import json
import logging

from typing import Union, Any

from utils import sys_exc


class SerdesBase:
    ENCODING = "utf-8"

    def __init__(self) -> None:
        pass

    def _encode(
        self,
        value: Union[dict, str],
        encoding: str = "utf-8",
    ) -> bytes:
        if isinstance(value, dict):
            value_encoded = json.dumps(value)
        else:
            value_encoded = str(value)
        try:
            result = value_encoded.encode(encoding)
        except LookupError:
            logging.error(sys_exc(sys.exc_info()))
            result = value_encoded.encode(SerdesBase.ENCODING)
        except Exception:
            logging.error(sys_exc(sys.exc_info()))
            result = value_encoded.encode(encoding, errors="ignore")
        return result

    def _decode(
        self,
        value: Union[str, bytes],
        encoding: str = "utf-8",
    ) -> str:
        if isinstance(value, bytes):
            try:
                result = value.decode(encoding)
            except LookupError:
                logging.error(sys_exc(sys.exc_info()))
                result = value.decode(SerdesBase.ENCODING)
            except Exception:
                logging.error(sys_exc(sys.exc_info()))
                result = value.decode(encoding, errors="ignore")
        else:
            result = value
        return result

    def serialise(
        self,
        value: Union[dict, str],
        encoding: str = "utf-8",
        schema_registry: Any = None,
        schema_str: str = None,
        kafka_topic: Any = None,
        message_field: Any = None,
    ) -> bytes:
        return self._encode(
            value,
            encoding=encoding,
        )

    def deserialise(
        self,
        value: Union[str, bytes],
        encoding: str = "utf-8",
        schema_registry: Any = None,
        kafka_topic: Any = None,
        message_field: Any = None,
    ) -> Union[dict, str]:
        return self._decode(
            value,
            encoding=encoding,
        )


def jsonify(
    value: Union[str, bytes],
    encoding: str = "utf-8",
) -> Union[str, dict]:
    """
    Load string as JSON or return it as binary decoded as string
    """
    if isinstance(value, bytes):
        value_decoded = SerdesBase()._decode(
            value,
            encoding=encoding,
        )
    else:
        value_decoded = value

    if isinstance(value_decoded, str):
        try:
            value_json = json.loads(value_decoded)
        except:
            pass

    if isinstance(value_json, dict):
        return value_json
    else:
        return value_decoded
