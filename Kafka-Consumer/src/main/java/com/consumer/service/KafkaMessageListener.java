package com.consumer.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaMessageListener {
	
	Logger logger = LoggerFactory.getLogger(KafkaMessageListener.class);
	
	@KafkaListener(topics = "newTopic2", groupId = "jt-group-1")
	public void consume1(String msg) {
	logger.info("consumer1 consumed: "+msg);	
	}
	
	@KafkaListener(topics = "newTopic2", groupId = "jt-group-1")
	public void consume2(String msg) {
	logger.info("consumer2 consumed: "+msg);	
	}
	
	@KafkaListener(topics = "newTopic2", groupId = "jt-group-1")
	public void consume3(String msg) {
	logger.info("consumer3 consumed: "+msg);	
	}
	
	@KafkaListener(topics = "newTopic2", groupId = "jt-group-1")
	public void consume4(String msg) {
	logger.info("consumer4 consumed: "+msg);	
	}
}
