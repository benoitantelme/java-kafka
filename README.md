# java-kafka
Java code around Kafka messaging framework
Code for simple producer and consumer committed. Code for simple broker and zookeeper too. Need to check how to stop/kill both of them at test startup.

Need to check how to bypass the sleep() in the test and check how to call the zookeeper stop script from java code.

Specific path to Kafka and Kafka logs in the broker properties:
log.dirs=D:/tmp/kafkalogs
main.log.dirs=D:/tmp/
kafka.program.path=C:\\PROGRA~1\\kafka_2.11-0.10.0.0\\bin\\windows\\