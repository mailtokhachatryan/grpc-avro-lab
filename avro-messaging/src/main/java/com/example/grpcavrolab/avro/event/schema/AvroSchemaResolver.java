package com.example.grpcavrolab.avro.event.schema;

import com.example.grpcavrolab.avro.event.inspection.ConfluentPayloadInspector;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.springframework.stereotype.Component;

@Component
public final class AvroSchemaResolver {
	private final SchemaRegistryClient registry;

	public AvroSchemaResolver(SchemaRegistryClient registry) {
		this.registry = registry;
	}

	/** Resolves the embedded ID under the topic's value subject. */
	public AvroSchema writerSchema(String subject, byte[] value) throws IOException {
		final int id = ConfluentPayloadInspector.schemaId(value);
		try {
			if (registry.getSchemaBySubjectAndId(subject, id) instanceof AvroSchema schema) {
				return schema;
			}
			throw new IOException("Schema ID " + id + " is not an Avro schema");
		} catch (RestClientException exception) {
			throw new IOException("Could not resolve schema ID " + id + " under " + subject, exception);
		}
	}

	/** Reads the original payload through the supplied reader schema without rewriting it. */
	public GenericRecord decode(String subject, byte[] value, Schema reader) throws IOException {
		final Schema writer = writerSchema(subject, value).rawSchema();
		final int offset = ConfluentPayloadInspector.PREFIX_LENGTH;
		final var decoder = DecoderFactory.get().binaryDecoder(value, offset, value.length - offset, null);
		return new GenericDatumReader<GenericRecord>(writer, reader).read(null, decoder);
	}
}
