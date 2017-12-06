package org.opennms.kafka.lag;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.opennms.kafka.model.KafkaLag;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class KafkaLagResource {

	public KafkaLagMonitor offsetMonitor;

	public KafkaLagResource() {
		offsetMonitor = new KafkaLagMonitor();
	}

	@RequestMapping(value = "/topics/{topic}/offset", method = RequestMethod.GET)
	public List<KafkaLag> getKafkaConsumerOffset(@PathVariable(value = "topic") String topic) {
		List<KafkaLag> kafkaOffsetMonitors = new ArrayList<>();
		Map<String, KafkaLag> map = offsetMonitor.getNewConsumer().get(topic);
		if (map != null) {
			kafkaOffsetMonitors.addAll(map.values());
		}
		return kafkaOffsetMonitors;
	}

}
