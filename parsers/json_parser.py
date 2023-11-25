from parsers import Parsers, jsonify


class Parser(Parsers):
    def __init__(self):
        super().__init__()

    def parse(
        self,
        obj,
        msg,
        encoding: str = "utf-8",
    ) -> tuple():
        # Return data as JSON
        parsed_headers = self.setHeaders(msg)
        parsed_topic = jsonify(
            msg.topic,
            encoding=encoding,
        )
        parsed_payload = jsonify(
            msg.payload,
            encoding=encoding,
        )
        return (
            parsed_headers,
            parsed_topic,
            parsed_payload,
        )
