package com.consumer.service;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;

import com.consumer.dto.Customer;

@Service
public class KafkaMessageListener {

	Logger logger = LoggerFactory.getLogger(KafkaMessageListener.class);

	/*
	 * @KafkaListener(topics = "newTopic2", groupId = "jt-group-1") public void
	 * consume1(String msg) { logger.info("consumer1 consumed: "+msg); }
	 * 
	 * @KafkaListener(topics = "newTopic2", groupId = "jt-group-1") public void
	 * consume2(String msg) { logger.info("consumer2 consumed: "+msg); }
	 * 
	 * @KafkaListener(topics = "newTopic2", groupId = "jt-group-1") public void
	 * consume3(String msg) { logger.info("consumer3 consumed: "+msg); }
	 * 
	 * @KafkaListener(topics = "newTopic2", groupId = "jt-group-1") public void
	 * consume4(String msg) { logger.info("consumer4 consumed: "+msg); }
	 */

	@KafkaListener(topics = "customer-tpoic2", groupId = "group_customer", containerFactory = "customerKafkaListenerFactory")
	public void listenCustomerEvents(Customer customer) {
		logger.info("customer event consumed: " + customer.toString());
	}

	@KafkaListener(topicPartitions = @TopicPartition(topic = "replication-topic1", partitions = {
			"2" }), groupId = "group_customer1", containerFactory = "stringKafkaListenerFactory")
	public void listenCustomerEvents_with_specific_partition(String msg) {
		logger.info("msg consumed: " + msg);
	}

	@RetryableTopic(attempts = "4",autoCreateTopics = "true" ,backoff = @Backoff(delay = 3000, multiplier = 1.5, maxDelay = 15000)) // 3 time retry attempt //(N-1)
	@KafkaListener(topics = "restrictedIp-topic", groupId = "ipCunsumer", containerFactory = "stringKafkaListenerFactory")
	public void restricetedIpListener(String IP, @Header("kafka_receivedTopic") String topic,
			@Header("kafka_receivedPartitionId") int partition, @Header("kafka_offset") long offset) {
		List<String> list_IP = List.of("1.1", "2.2", "3.3");
		if (list_IP.contains(IP.replace("\"", "").trim())) {
			throw new RuntimeException("IP" + IP + " is restricted");
		} else {
			logger.info(IP + " is not restriced.");
		}
	}
	
	@DltHandler
	public void listenDLT(String IP, @Header("kafka_receivedTopic") String topic,
			@Header("kafka_receivedPartitionId") int partition, @Header("kafka_offset") long offset) {
		logger.warn("DLT revieved: {} , from {}, offset {}", IP, topic, offset);
	}

}
