package com.consumer.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
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

	/*
	 * @KafkaListener(topics = "customer-tpoic2", groupId = "group_customer",
	 * containerFactory = "customerKafkaListenerFactory") public void
	 * listenCustomerEvents(Customer customer) {
	 * logger.info("customer event consumed: " + customer.toString()); }
	 */

	@KafkaListener(topicPartitions = @TopicPartition(topic = "replication-topic1", partitions = {
			"2" }),
			groupId = "group_customer1", containerFactory = "stringKafkaListenerFactory")
	public void listenCustomerEvents_with_specific_partition(String msg) {
		logger.info("msg consumed: " + msg);
	}
}
