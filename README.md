# kafka-demo

Shows how to configure Spring Kafka and Spring Boot to send messages using JSON and receive them in multiple formats: JSON, plain Strings or byte arrays.

This sample application also demonstrates how to use multiple Kafka consumers within the same consumer group with the @KafkaListener annotation, 
so the messages are load-balanced. Each consumer implements a different deserialization approach.
