package org.opennms.kafka.lag;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.opennms.kafka.model.KafkaLag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;

public class KafkaLagMonitor {

	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaLagMonitor.class);
	public static final String OFFSET = "offset";
	public static final String TOPIC = "topic";
	public static final String CONSUMER_GROUP_NAME = "_kafka_monitor";

	private static final Map<String, SimpleConsumer> consumerMap = new HashMap<String, SimpleConsumer>();
	private static final String clientName = "GetOffsetClient";
	private Map<String, Map<String, KafkaLag>> consumerOffsetMap = new ConcurrentHashMap<>();
	private KafkaNewConsumerOffsetThread kafkaConsumerOffsetThread;
	public Thread th;

	public KafkaLagMonitor() {
		super();
		kafkaConsumerOffsetThread = new KafkaNewConsumerOffsetThread(this);
		th = new Thread(kafkaConsumerOffsetThread);
		th.setDaemon(true);
		th.start();
	}

	public SimpleConsumer getConsumer(String host, int port, String clientName) {
		SimpleConsumer consumer = consumerMap.get(host);
		if (consumer == null) {
			consumer = new SimpleConsumer(host, port, 100000, 64 * 1024, clientName);
			System.out.println("Created a new Kafka Consumer for host: " + host);
			consumerMap.put(host, consumer);
		}
		return consumer;
	}

	public Map<String, Map<String, KafkaLag>> getNewConsumer() {
		return consumerOffsetMap;
	}

	private class KafkaNewConsumerOffsetThread implements Runnable {

		private KafkaLagMonitor util;
		private final AtomicBoolean closed = new AtomicBoolean(false);
		private KafkaConsumer<byte[], byte[]> consumer;

		public KafkaNewConsumerOffsetThread(KafkaLagMonitor util) {
			this.util = util;
		}

		@Override
		public void run() {
			try {
				Properties props = new Properties();
				props.put("bootstrap.servers", "localhost:9092");
				props.put("group.id", CONSUMER_GROUP_NAME);
				props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
				props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
				props.put("enable.auto.commit", "false");
				props.put("auto.offset.reset", "latest");
				consumer = new KafkaConsumer<>(props);
				consumer.subscribe(Arrays.asList("__consumer_offsets"));
				System.out.println("Connected to Kafka consumer offset topic");
				Schema schema = new Schema(new Field("group", Schema.STRING), new Field(TOPIC, Schema.STRING),
						new Field("partition", Schema.INT32));
				while (!closed.get()) {
					ConsumerRecords<byte[], byte[]> records = consumer.poll(1000);
					for (ConsumerRecord<byte[], byte[]> consumerRecord : records) {
						if (consumerRecord.value() != null && consumerRecord.key() != null) {
							ByteBuffer key = ByteBuffer.wrap(consumerRecord.key());
							short version = key.getShort();
							if (version < 2) {
								try {
									Struct struct = (Struct) schema.read(key);
									if (struct.getString("group").equalsIgnoreCase(CONSUMER_GROUP_NAME)) {
										continue;
									}
									String group = struct.getString("group");
									String topic = struct.getString(TOPIC);
									int partition = struct.getInt("partition");
									SimpleConsumer con = util.getConsumer("localhost", 9092, clientName);
									long realOffset = util.getLastOffset(con, struct.getString(TOPIC), partition, -1,
											clientName);
									long consumerOffset = readOffsetMessageValue(
											ByteBuffer.wrap(consumerRecord.value()));
									long lag = realOffset - consumerOffset;
									KafkaLag mon = new KafkaLag(group, topic, partition, realOffset,
											consumerOffset, lag);
									LOGGER.info(" consumerOffset:" + consumerOffset + "  realOffset: " + realOffset
											+ "   lag:  " + Long.toString(lag));
									Map<String, KafkaLag> map = consumerOffsetMap.get(topic);
									if (map == null) {
										map = new ConcurrentHashMap<>();
										consumerOffsetMap.put(topic, map);
									}
									map.put(group + "%" + partition, mon);

								} catch (Exception e) {
									e.printStackTrace();
								}
							}
						}
					}
				}
			} catch (Exception e) {
				// e.printStackTrace();
			} finally {
				consumer.close();
			}

		}

		// Shutdown hook which can be called from a separate thread
		public void shutdown() {
			closed.set(true);
			if (consumer != null) {
				consumer.wakeup();
			}
		}

	}

	private long readOffsetMessageValue(ByteBuffer buffer) {
		buffer.getShort(); // read and ignore version
		long offset = buffer.getLong();
		return offset;
	}

	public long getLastOffset(SimpleConsumer consumer, String topic, int partition, long whichTime, String clientName) {
		long lastOffset = 0;
		try {
			List<String> topics = Collections.singletonList(topic);
			TopicMetadataRequest req = new TopicMetadataRequest(topics);
			kafka.javaapi.TopicMetadataResponse topicMetadataResponse = consumer.send(req);
			TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
			for (TopicMetadata topicMetadata : topicMetadataResponse.topicsMetadata()) {
				for (PartitionMetadata partitionMetadata : topicMetadata.partitionsMetadata()) {
					if (partitionMetadata.partitionId() == partition) {
						String partitionHost = partitionMetadata.leader().host();
						consumer = getConsumer(partitionHost, partitionMetadata.leader().port(), clientName);
						break;
					}
				}
			}
			Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
			requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
			kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo,
					kafka.api.OffsetRequest.CurrentVersion(), clientName);
			OffsetResponse response = consumer.getOffsetsBefore(request);
			if (response.hasError()) {
				System.out.println(
						"Error fetching Offset Data from the Broker. Reason: " + response.errorCode(topic, partition));
				lastOffset = 0;
			}
			long[] offsets = response.offsets(topic, partition);
			lastOffset = offsets[0];
		} catch (Exception e) {
			System.out.println("Error while collecting the log Size for topic: " + topic + ", and partition: "
					+ partition + e.getMessage());
		}
		return lastOffset;
	}

	public void closeConnection() throws InterruptedException {
		kafkaConsumerOffsetThread.shutdown();
		for (SimpleConsumer consumer : consumerMap.values()) {
			System.out.println("Closing connection for: " + consumer.host());
			consumer.close();
		}
		th.join();
	}

}
