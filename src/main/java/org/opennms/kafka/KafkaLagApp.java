package org.opennms.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaLagApp {

	public static void main(String[] args) throws InterruptedException {
        SpringApplication.run(KafkaLagApp.class, args);     
    }

}
