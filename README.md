# Testing with Spring Embedded Kafka

I created this project to demonstrate a bug in Spring Kafka's ```EmbeddedKafka```. See ```KafkaEmbeddedTest```.

When a consumer is created with ```EmbeddedKafka.consumeFromAllEmbeddedTopics(Consumer consumer)```, all the  messages are received.

When a consumer is created with ```EmbeddedKafka.consumeFromAnEmbeddedTopic(Consumer consumer, String topic)```, the messages are not all received.

I believe this is because ```consumeFromAnEmbeddedTopic``` does not wait to be assigned partitions from the embedded topic as ```consumeFromAllEmbeddedTopics```.
