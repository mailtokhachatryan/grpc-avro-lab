package com.example.grpcavrolab.avro.event;

import com.example.grpcavrolab.avro.MessagingOptions;
import com.example.grpcavrolab.avro.config.KafkaClients;
import com.example.grpcavrolab.avro.config.MessagingProperties;
import com.example.grpcavrolab.avro.event.consumer.MessageConsumer;
import com.example.grpcavrolab.avro.event.inspection.ConfluentPayloadInspector;
import com.example.grpcavrolab.avro.event.producer.MessageProducer;
import com.example.grpcavrolab.avro.event.schema.AvroSchemaResolver;
import com.example.grpcavrolab.avro.event.schema.MessageSchemas;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;

@Component
public final class MessagingCommands {
	private final MessagingProperties properties;
	private final KafkaClients clients;
	private final MessageSchemas schemas;
	private final MessageProducer producer;
	private final MessageConsumer consumer;
	private final AvroSchemaResolver resolver;
	private final SchemaRegistryClient registry;

	public MessagingCommands(MessagingProperties properties, KafkaClients clients, MessageSchemas schemas,
		MessageProducer producer, MessageConsumer consumer, AvroSchemaResolver resolver, SchemaRegistryClient registry) {
		this.properties = properties;
		this.clients = clients;
		this.schemas = schemas;
		this.producer = producer;
		this.consumer = consumer;
		this.resolver = resolver;
		this.registry = registry;
	}

	/** Executes one bounded command. An output failure prevents an offset commit. */
	public void execute(MessagingOptions options, PrintStream output)
		throws IOException, RestClientException, InterruptedException, ExecutionException, TimeoutException {
		switch (options.command()) {
			case "produce" -> produce(options, output);
			case "consume" -> consume(options, output);
			case "inspect" -> inspect(options, output);
			default -> throw new IllegalArgumentException("Unrecognized messaging command");
		}
	}

	private void produce(MessagingOptions options, PrintStream output)
		throws IOException, RestClientException, InterruptedException, ExecutionException, TimeoutException {
		final Schema schema = schemas.writer(options.writerVersion());
		final GenericRecord event = schemas.event(schema, options.eventId(), options.message(), options.source());
		final var delivery = producer.send(event);
		final var metadata = delivery.metadata();
		output.printf("produced topic=%s partition=%d offset=%d writer=%s schemaId=%d subjectVersion=%d event=%s%n",
			metadata.topic(), metadata.partition(), metadata.offset(), options.writerVersion(), delivery.schemaId(),
			delivery.subjectVersion(), event);
		checkOutput(output);
	}

	private void consume(MessagingOptions options, PrintStream output) throws IOException {
		final int count = options.count();
		final String readerVersion = options.readerVersion();
		if ("writer".equals(readerVersion)) {
			consumer.consume(clients.avroConsumer(), count,
				record -> printRecord(output, record, record.value(), readerVersion));
			return;
		}
		final Schema reader = schemas.reader(readerVersion);
		consumer.consume(clients.rawConsumer(), count,
			record -> printRecord(output, record, resolver.decode(properties.subject(), record.value(), reader), readerVersion));
	}

	private void inspect(MessagingOptions options, PrintStream output) throws IOException {
		final int count = options.count();
		consumer.inspect(clients.inspector(), count, record -> {
			final byte[] value = record.value();
			final int id = ConfluentPayloadInspector.schemaId(value);
			final var schema = resolver.writerSchema(properties.subject(), value);
			final int version;
			try {
				version = registry.getVersion(properties.subject(), schema);
			} catch (RestClientException exception) {
				throw new IOException("Could not read Registry version for schema ID " + id, exception);
			}
			output.printf("inspected topic=%s partition=%d offset=%d schemaId=%d subjectVersion=%d prefix=%s schema=%s%n",
				record.topic(), record.partition(), record.offset(), id, version,
				ConfluentPayloadInspector.prefixHex(value), schema.canonicalString());
			checkOutput(output);
		});
	}

	private void printRecord(PrintStream output, ConsumerRecord<String, ?> record, GenericRecord value, String reader)
		throws IOException {
		if (value == null) {
			throw new IOException("Unexpected null value in the message event topic");
		}
		output.printf("consumed topic=%s partition=%d offset=%d reader=%s event=%s%n",
			record.topic(), record.partition(), record.offset(), reader, value);
		checkOutput(output);
	}

	private void checkOutput(PrintStream output) throws IOException {
		if (output.checkError()) {
			throw new IOException("Could not write the record to stdout");
		}
	}
}
