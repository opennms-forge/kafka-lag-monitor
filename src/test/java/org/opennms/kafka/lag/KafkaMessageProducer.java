package org.opennms.kafka.lag;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Component;

import static org.opennms.kafka.KafkaLagConstants.*;

@Component
public class KafkaMessageProducer {

    public void produce() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord<String, String>(USER_TOPIC, Integer.toString(i),
                    "This is  " + Integer.toString(i) + "  Message"));
        }

        producer.close();
    }

    public static void main(String[] args) throws Exception {
        new KafkaMessageProducer().produce();
    }
}
