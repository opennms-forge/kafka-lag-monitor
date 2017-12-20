package org.opennms.kafka.lag;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import static org.opennms.kafka.KafkaLagConstants.*;

@Component
public class KafkaMessageConsumer implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaMessageConsumer.class);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private KafkaConsumer<String, String> consumer;

    @Override
    public void run() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", USER_GROUP);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(USER_TOPIC));
        while (!closed.get()) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                LOGGER.info(record.value());
            }
        }
    }

    // Shutdown hook which can be called from a separate thread
    public void shutdown() {
        closed.set(true);
        if (consumer != null) {
            consumer.wakeup();
        }
    }

    public static void main(String[] args) throws Exception {

        KafkaMessageConsumer messageConsumer = new KafkaMessageConsumer();
        Thread consumerThread = new Thread(messageConsumer);
        consumerThread.setDaemon(true);
        consumerThread.start();
        Thread.sleep(10000);
        messageConsumer.shutdown();
    }
}
