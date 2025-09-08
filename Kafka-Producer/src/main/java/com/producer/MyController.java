package com.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.producer.dto.Customer;
import com.producer.kafka_service.KafkaMessagePublisher;

@RestController
@RequestMapping("/producer-app")
public class MyController {

	@Autowired
	private KafkaMessagePublisher kafkaMessagePublisher;

	@GetMapping("/publish/{msg}")
	public ResponseEntity<?> publishMessage(@PathVariable String msg) {
		try {
			for(int i =0; i <=100000; i++) {
			kafkaMessagePublisher.sendMessageToKafkaTopic(msg+i);
			}
			return ResponseEntity.ok("message sent: " + msg);
		} catch (Exception e) {
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
		}
	}
	
	@PostMapping("/publishEvent")
	public ResponseEntity<?> publishEvent(@RequestBody Customer customer){
		kafkaMessagePublisher.sendEvents(customer);
		return ResponseEntity.ok("event sent: "+ customer.toString());
	}
}
