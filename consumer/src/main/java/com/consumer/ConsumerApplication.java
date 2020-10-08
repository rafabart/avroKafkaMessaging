package com.consumer;

import com.example.CustomerV1;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Collections;
import java.util.Properties;

@SpringBootApplication
public class ConsumerApplication {

	public static void main(String[] args) {
		SpringApplication.run(ConsumerApplication.class, args);

		Properties properties = new Properties();
		// normal consumer
		properties.setProperty("bootstrap.servers","127.0.0.1:9092");
		properties.put("group.id", "customer-consumer-group-v1");
		properties.put("auto.commit.enable", "false");
		properties.put("auto.offset.reset", "earliest");

		// avro part (deserializer)
		properties.setProperty("key.deserializer", StringDeserializer.class.getName());
		properties.setProperty("value.deserializer", KafkaAvroDeserializer.class.getName());
		properties.setProperty("schema.registry.url", "http://127.0.0.1:8081");
		properties.setProperty("specific.avro.reader", "true");

		KafkaConsumer<String, CustomerV1> kafkaConsumer = new KafkaConsumer<>(properties);
		String topic = "customer-avro";
		kafkaConsumer.subscribe(Collections.singleton(topic));

		System.out.println("Waiting for data...");

		while (true){
			System.out.println("Polling");
			ConsumerRecords<String, CustomerV1> records = kafkaConsumer.poll(1000);

			for (ConsumerRecord<String, CustomerV1> record : records){
				CustomerV1 customer = record.value();
				System.out.println(customer);
			}

			kafkaConsumer.commitSync();
		}
	}

}
