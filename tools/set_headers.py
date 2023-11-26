import sys
import logging

from tools import ToolsBase
from utils import sys_exc


class Tools(ToolsBase):
    def __init__(self):
        super().__init__()

    def do(
        self,
        msg,
    ) -> dict:
        try:
            return {
                "mqtt.message.id": str(msg.mid),
                "mqtt.qos": str(msg.qos),
                "mqtt.retained": "true" if msg.retain > 0 else "false",
                "mqtt.duplicate": "true" if msg.dup > 0 else "false",
            }
        except Exception:
            logging.error(sys.exc_info())
            return dict()
