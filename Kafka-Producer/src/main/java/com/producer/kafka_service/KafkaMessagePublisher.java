package com.producer.kafka_service;

import java.util.concurrent.CompletableFuture;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.AutoConfigureOrder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

@Service
public class KafkaMessagePublisher {

	@Autowired
	private KafkaTemplate<String, Object> template;
	
	public void sendMessageToKafkaTopic(String msg) {
		CompletableFuture<SendResult<String, Object>> send = template.send("newTopic2", msg);
		send.whenComplete((result, ex) -> {
			if(ex == null) {
				System.out.println("sent message: " + msg + " with offset: "+ result.getRecordMetadata().offset());
			}
			else {
				System.out.println("Unable to send message: "+ msg + " due to " + ex.getMessage());
			}
		});
	}
}
 