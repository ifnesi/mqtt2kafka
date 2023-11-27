import logging

from mq.mqtt import MQTT


if __name__ == "__main__":
    # Logging config
    logging.basicConfig(
        format="%(asctime)s.%(msecs)03d [%(levelname)s]: %(message)s",
        level=logging.INFO,
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    MQTT(
        "config/example.yaml",
        log=True,
    )
