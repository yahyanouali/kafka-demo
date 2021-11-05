package com.kafka.demo.cntroller;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.kafka.demo.model.PracticalAdvice;

@RestController
public class HelloKafkaProducerConsumerController {

    private static final Logger logger = LoggerFactory.getLogger(HelloKafkaProducerConsumerController.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String adviceTopicName;
    private final int messagesPerRequest;
    private CountDownLatch latch;

    public HelloKafkaProducerConsumerController(KafkaTemplate<String, Object> kafkaTemplate,
                                                @Value("${demo.kafka.topic-name}") String adviceTopicName,
                                                @Value("${demo.kafka.messages-per-request}") int messagesPerRequest) {
        this.kafkaTemplate = kafkaTemplate;
        this.adviceTopicName = adviceTopicName;
        this.messagesPerRequest = messagesPerRequest;
    }

    @GetMapping("/hello")
    public String hello() throws InterruptedException {
        latch = new CountDownLatch(messagesPerRequest);

        IntStream.range(0, messagesPerRequest)
                .forEach(i -> this.kafkaTemplate.send(adviceTopicName,
                        String.valueOf(i),
                        new PracticalAdvice("A Practical Advice", i))
                );

        boolean await = latch.await(60, TimeUnit.SECONDS);

        if(await) {
            logger.info("All messages received");
        } else {
            logger.warn("Not all messages was received");
        }

        return "Hello Kafka!";

    }

    @KafkaListener(topics = "advice-topic", clientIdPrefix = "json", containerFactory = "kafkaListenerContainerFactory")
    public void listenAsObject(ConsumerRecord<String, PracticalAdvice> cr, @Payload PracticalAdvice payload) {
        if(logger.isInfoEnabled()) {
            logger.info("Logger 1 [JSON] received key {}: Type [{}] | Payload: {} | Record: {}", cr.key(),
                    typeIdHeader(cr.headers()), payload, cr);
        }

        latch.countDown();
    }

    @KafkaListener(topics = "advice-topic", clientIdPrefix = "string", containerFactory = "kafkaListenerStringContainerFactory")
    public void listenasString(ConsumerRecord<String, String> cr, @Payload String payload) {
        if(logger.isInfoEnabled()) {
            logger.info("Logger 2 [String] received key {}: Type [{}] | Payload: {} | Record: {}", cr.key(),
                    typeIdHeader(cr.headers()), payload, cr);
        }

        latch.countDown();
    }

    @KafkaListener(topics = "advice-topic", clientIdPrefix = "bytearray", containerFactory = "kafkaListenerByteArrayContainerFactory")
    public void listenAsByteArray(ConsumerRecord<String, byte[]> cr, @Payload byte[] payload) {
        if(logger.isInfoEnabled()) {
            logger.info("Logger 3 [ByteArray] received key {}: Type [{}] | Payload: {} | Record: {}", cr.key(),
                    typeIdHeader(cr.headers()), payload, cr);
        }
        latch.countDown();
    }

    private static String typeIdHeader(Headers headers) {
        return StreamSupport.stream(headers.spliterator(), false)
                .filter(header -> header.key().equals("__TypeId__"))
                .findFirst().map(header -> new String(header.value())).orElse("N/A");
    }

}
