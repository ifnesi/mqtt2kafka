import json
import random
import datetime

import paho.mqtt.client as mqtt


def on_connect(mqttc, obj, flags, rc):
    print("rc: " + str(rc))


def on_message(mqttc, obj, msg):
    print(msg.topic + " " + str(msg.qos) + " " + str(msg.payload))


def on_publish(mqttc, obj, mid):
    print("mid: " + str(mid))


def on_subscribe(mqttc, obj, mid, granted_qos):
    print("Subscribed: " + str(mid) + " " + str(granted_qos))


mqttc = mqtt.Client(client_id=None, clean_session=True)
mqttc.on_message = on_message
mqttc.on_connect = on_connect
mqttc.on_publish = on_publish
mqttc.on_subscribe = on_subscribe
mqttc.connect("localhost", 1883, 60)
mqttc.loop_start()

infot = mqttc.publish(
    "/iot/sensor/light/m5/data/b0b21c60674c",
    json.dumps(
        {
            "timestamp": datetime.datetime.utcnow().isoformat().replace("T", " ")[:23],
            "lux": random.randint(100, 99999) / 100,
            "dummy_field": "DROP ME!",
        }
    ),
    qos=0,
)

infot = mqttc.publish(
    "/iot/sensor/env/m5/data/b0b21c60674c",
    json.dumps(
        {
            "timestamp": datetime.datetime.utcnow().isoformat().replace("T", " ")[:23],
            "temperature": random.randint(-1000, 9999) / 100,
            "humidity": random.randint(0, 9999) / 100,
            "dummy_field": "DROP ME!",
        }
    ),
    qos=0,
)

infot = mqttc.publish(
    "/iot/sensor/pir/m5/data/b0b21c60674c",
    json.dumps(
        {
            "timestamp": datetime.datetime.utcnow().isoformat().replace("T", " ")[:23],
            "pir_state": random.random() > 0.5,
            "dummy_field": "DROP ME!",
        }
    ),
    qos=0,
)

infot.wait_for_publish()
