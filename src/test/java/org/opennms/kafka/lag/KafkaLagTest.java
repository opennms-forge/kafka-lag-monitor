package org.opennms.kafka.lag;

import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.hamcrest.collection.IsEmptyCollection;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opennms.kafka.KafkaMessageConsumer;
import org.opennms.kafka.KafkaMessageProducer;
import org.opennms.kafka.model.KafkaLag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.junit4.SpringRunner;
import static org.opennms.kafka.KafkaLagConstants.*;

import com.fasterxml.jackson.core.JsonProcessingException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
public class KafkaLagTest {
	

	@Autowired
	private TestRestTemplate restTemplate;
	
	@Before
	public void init() throws InterruptedException {
		
		KafkaMessageProducer producer = new KafkaMessageProducer();
		KafkaMessageConsumer messageConsumer = new KafkaMessageConsumer();
		Thread consumerThread = new Thread(messageConsumer);
		consumerThread.setDaemon(true);
		consumerThread.start();
		Thread.sleep(5000);
		producer.produce();
		Thread.sleep(5000);
	}
	
	@Test
	public void testKafkaLag() throws JsonProcessingException, IOException {
		
		ResponseEntity<KafkaLag[]> response = this.restTemplate.getForEntity(String.format("/topics/%s/offset",USER_TOPIC), KafkaLag[].class);
		assertThat(response.getStatusCode(), equalTo(HttpStatus.OK));
		List<KafkaLag> offsetList = Arrays.asList(response.getBody());
		assertThat(offsetList, not(IsEmptyCollection.empty()));
		for ( KafkaLag offset : offsetList) {
			assertThat(offset.getLag(), equalTo(offset.getLogSize() - offset.getConsumerOffset()));
		}
	}


}
