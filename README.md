# mqtt2kafka

Experimental Python Source/Sink connector with custom Deserialiser/SMT/Serialiser between MQTT and Kafka

Source: [MQTT_BROKER] --> MQTT_Deserialiser --> SMT --> Kafka_Serialiser --> [Kafka]
Sink: [Kafka] --> Kafka_Deserialiser --> MQTT_Serialiser --> [MQTT_BROKER]

THIS IS A WORK IN PROGRESS, DON'T USE IT YET!