package com.example.grpcavrolab.avro.event.schema;

import com.example.grpcavrolab.avro.event.inspection.ConfluentPayloadInspector;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.util.Map;
import org.apache.avro.AvroTypeException;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AvroSchemaResolverTest {
	private static final String TOPIC = "lab.unit";
	private static final String SUBJECT = TOPIC + "-value";
	private final MessageSchemas schemas = new MessageSchemas();

	@Test
	void usesActualWriterIdsAndProjectsBothVersionsWithoutChangingThePayload() throws Exception {
		try (var registry = new MockSchemaRegistryClient(); var serializer = new KafkaAvroSerializer(registry)) {
			serializer.configure(Map.of("schema.registry.url", "mock://unit"), false);
			final byte[] v1 = serializer.serialize(TOPIC, schemas.event(schemas.writer("v1"), "one", "hello", null));
			final byte[] v2 = serializer.serialize(TOPIC, schemas.event(schemas.writer("v2"), "two", "hello", "java"));
			final byte[] original = v1.clone();
			final var resolver = new AvroSchemaResolver(registry);

			assertNotEquals(ConfluentPayloadInspector.schemaId(v1), ConfluentPayloadInspector.schemaId(v2));
			for (String reader : new String[]{"v1", "v2"}) {
				final GenericRecord first = resolver.decode(SUBJECT, v1, schemas.reader(reader));
				final GenericRecord second = resolver.decode(SUBJECT, v2, schemas.reader(reader));
				assertEquals("one", first.get(MessageSchemas.EVENT_ID).toString());
				assertEquals("two", second.get(MessageSchemas.EVENT_ID).toString());
				if ("v2".equals(reader)) {
					assertNull(first.get(MessageSchemas.SOURCE));
					assertEquals("java", second.get(MessageSchemas.SOURCE).toString());
				} else {
					assertNull(first.getSchema().getField(MessageSchemas.SOURCE));
					assertNull(second.getSchema().getField(MessageSchemas.SOURCE));
				}
			}
			assertArrayEquals(original, v1);
			assertThrows(AvroTypeException.class, () -> resolver.decode(SUBJECT, v1, schemas.reader("incompatible")));
		}
	}
}
