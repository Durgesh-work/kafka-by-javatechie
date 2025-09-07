package com.consumer.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaMessageListener {
	
	Logger logger = LoggerFactory.getLogger(KafkaMessageListener.class);
	
	@KafkaListener(topics = "newTopic2", groupId = "jt-group-1")
	public void consume(String msg) {
	logger.info("consumer consumed: "+msg);	
	}
}
