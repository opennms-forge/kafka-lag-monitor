package org.opennms.kafka;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

@Component
@PropertySource("classpath:application.properties")
public class KafkaConfig {

    @Value("${kafkaHost}")
    private String kafkaHost;

    public String getKafkaHost() {
        return kafkaHost;
    }

    @Override
    public String toString() {
        return "KafkaConfig [kafkaHost=" + kafkaHost + "]";
    }

}
