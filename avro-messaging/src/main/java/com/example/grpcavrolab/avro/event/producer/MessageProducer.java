package com.example.grpcavrolab.avro.event.producer;

import com.example.grpcavrolab.avro.config.KafkaClients;
import com.example.grpcavrolab.avro.config.MessagingProperties;
import com.example.grpcavrolab.avro.event.schema.MessageSchemas;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.stereotype.Component;

@Component
public final class MessageProducer {
	private final KafkaClients clients;
	private final MessagingProperties properties;
	private final SchemaRegistryClient registry;

	public MessageProducer(KafkaClients clients, MessagingProperties properties, SchemaRegistryClient registry) {
		this.clients = clients;
		this.properties = properties;
		this.registry = registry;
	}

	/** Waits for the broker acknowledgement, then looks up the exact writer schema. */
	public Delivery send(GenericRecord event) throws InterruptedException, ExecutionException, TimeoutException,
		IOException, RestClientException {
		final String key = event.get(MessageSchemas.EVENT_ID).toString();
		final KafkaProducer<String, GenericRecord> producer = clients.producer();
		final RecordMetadata metadata;
		try {
			metadata = producer.send(new ProducerRecord<>(properties.topic(), key, event))
				.get(properties.timeout().toMillis(), TimeUnit.MILLISECONDS);
		} finally {
			producer.close(properties.closeTimeout());
		}
		final AvroSchema schema = new AvroSchema(event.getSchema());
		try {
			return new Delivery(metadata, registry.getId(properties.subject(), schema),
				registry.getVersion(properties.subject(), schema));
		} catch (IOException | RestClientException exception) {
			throw new IOException("Kafka acknowledged partition " + metadata.partition() + " offset " + metadata.offset()
				+ ", but Registry metadata could not be read; do not retry the send blindly", exception);
		}
	}

	public record Delivery(RecordMetadata metadata, int schemaId, int subjectVersion) {}
}
