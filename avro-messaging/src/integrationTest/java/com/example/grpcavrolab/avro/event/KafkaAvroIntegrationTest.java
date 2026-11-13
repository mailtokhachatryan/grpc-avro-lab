package com.example.grpcavrolab.avro.event;

import com.example.grpcavrolab.avro.config.KafkaClients;
import com.example.grpcavrolab.avro.config.MessagingProperties;
import com.example.grpcavrolab.avro.event.consumer.MessageConsumer;
import com.example.grpcavrolab.avro.event.inspection.ConfluentPayloadInspector;
import com.example.grpcavrolab.avro.event.producer.MessageProducer;
import com.example.grpcavrolab.avro.event.schema.AvroSchemaResolver;
import com.example.grpcavrolab.avro.event.schema.MessageSchemas;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.ConfluentKafkaContainer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaAvroIntegrationTest {
	private static final String CONFLUENT_TAG = "8.4.1";
	private static final String INTERNAL_LISTENER = "kafka:19092";
	private static final int REGISTRY_PORT = 8081;
	private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(5);

	private static final Network NETWORK = Network.newNetwork();
	private static final ConfluentKafkaContainer KAFKA = new ConfluentKafkaContainer("confluentinc/cp-kafka:" + CONFLUENT_TAG)
		.withNetwork(NETWORK)
		.withNetworkAliases("kafka")
		.withListener(INTERNAL_LISTENER)
		.withEnv("KAFKA_HEAP_OPTS", "-Xms256m -Xmx512m");
	private static final GenericContainer<?> REGISTRY = new GenericContainer<>("confluentinc/cp-schema-registry:" + CONFLUENT_TAG)
		.withNetwork(NETWORK)
		.withExposedPorts(REGISTRY_PORT)
		.withEnv("SCHEMA_REGISTRY_HOST_NAME", "registry")
		.withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:" + REGISTRY_PORT)
		.withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://" + INTERNAL_LISTENER)
		.withEnv("SCHEMA_REGISTRY_KAFKASTORE_TOPIC_REPLICATION_FACTOR", "1")
		.withEnv("SCHEMA_REGISTRY_HEAP_OPTS", "-Xms128m -Xmx256m")
		.waitingFor(Wait.forHttp("/subjects").forPort(REGISTRY_PORT).withStartupTimeout(Duration.ofMinutes(2)));

	private final MessageSchemas schemas = new MessageSchemas();

	@BeforeAll
	static void startServices() {
		KAFKA.start();
		REGISTRY.start();
	}

	@AfterAll
	static void stopServices() {
		try {
			REGISTRY.stop();
		} finally {
			try {
				KAFKA.stop();
			} finally {
				NETWORK.close();
			}
		}
	}

	@Test
	@Timeout(90)
	void verifiesKafkaOffsetsAndRegistryBackedSchemaEvolution() throws Exception {
		final String topic = "lab.test." + UUID.randomUUID();
		final URI registryUrl = URI.create("http://" + REGISTRY.getHost() + ":" + REGISTRY.getMappedPort(REGISTRY_PORT));
		final var settings = new MessagingProperties(List.of(KAFKA.getBootstrapServers()), registryUrl, topic,
			"test-" + UUID.randomUUID(), Duration.ofSeconds(20), REQUEST_TIMEOUT, Duration.ofMillis(100),
			REQUEST_TIMEOUT, 32);
		createTopic(settings);

		try (var registry = new CachedSchemaRegistryClient(registryUrl.toString(), settings.registryCacheCapacity())) {
			registry.updateCompatibility(settings.subject(), "BACKWARD_TRANSITIVE");
			assertEquals("BACKWARD_TRANSITIVE", registry.getCompatibility(settings.subject()));
			final var clients = new KafkaClients(settings);
			final var producer = new MessageProducer(clients, settings, registry);
			final var consumer = new MessageConsumer(settings);
			final var first = producer.send(schemas.event(schemas.writer("v1"), "one", "old", null));
			final var second = producer.send(schemas.event(schemas.writer("v2"), "two", "new", "java"));

			assertNotEquals(first.schemaId(), second.schemaId());
			assertTrue(second.subjectVersion() > first.subjectVersion());
			assertEquals(first.metadata().offset() + 1, second.metadata().offset());

			final List<String> printed = new ArrayList<>();
			final IOException failure = assertThrows(IOException.class,
				() -> consumer.consume(clients.avroConsumer(), 2, record -> {
					if ("two".equals(record.key())) {
						throw new IOException("Simulated output failure");
					}
					printed.add(record.value().get(MessageSchemas.MESSAGE).toString());
				}));
			assertTrue(failure.getMessage().contains("offset " + second.metadata().offset()));
			assertEquals(List.of("old"), printed);
			assertCommitted(settings, second.metadata().offset());

			final List<ConsumerRecord<String, byte[]>> values = new ArrayList<>();
			consumer.inspect(clients.inspector(), 2, values::add);
			assertCommitted(settings, second.metadata().offset());
			final byte[] rawV1 = values.getFirst().value();
			final byte[] rawV2 = values.getLast().value();
			final byte[] originalV1 = rawV1.clone();
			assertEquals(first.schemaId(), ConfluentPayloadInspector.schemaId(rawV1));
			assertEquals(second.schemaId(), ConfluentPayloadInspector.schemaId(rawV2));
			assertEquals(first.schemaId(), registry.getSchemaMetadata(settings.subject(), first.subjectVersion()).getId());
			assertEquals(second.schemaId(), registry.getSchemaMetadata(settings.subject(), second.subjectVersion()).getId());

			final var resolver = new AvroSchemaResolver(registry);
			assertNull(resolver.decode(settings.subject(), rawV1, schemas.reader("v2")).get(MessageSchemas.SOURCE));
			assertNull(resolver.decode(settings.subject(), rawV2, schemas.reader("v1")).getSchema().getField(MessageSchemas.SOURCE));
			assertArrayEquals(originalV1, rawV1);

			final List<GenericRecord> replayed = new ArrayList<>();
			consumer.consume(clients.rawConsumer(), 1,
				record -> replayed.add(resolver.decode(settings.subject(), record.value(), schemas.reader("v2"))));
			assertEquals("two", replayed.getFirst().get(MessageSchemas.EVENT_ID).toString());
			assertEquals("java", replayed.getFirst().get(MessageSchemas.SOURCE).toString());
			assertCommitted(settings, second.metadata().offset() + 1);

			final List<Integer> versions = registry.getAllVersions(settings.subject());
			final AvroSchema bad = new AvroSchema(schemas.reader("incompatible"));
			assertFalse(registry.testCompatibility(settings.subject(), bad));
			final RestClientException rejected = assertThrows(RestClientException.class,
				() -> registry.register(settings.subject(), bad));
			assertEquals(409, rejected.getStatus());
			assertEquals(versions, registry.getAllVersions(settings.subject()));
			System.out.printf("Registry verified: v%d id=%d prefix=%s; v%d id=%d prefix=%s; incompatible registration HTTP %d%n",
				first.subjectVersion(), first.schemaId(), ConfluentPayloadInspector.prefixHex(rawV1),
				second.subjectVersion(), second.schemaId(), ConfluentPayloadInspector.prefixHex(rawV2), rejected.getStatus());
		}
	}

	private void createTopic(MessagingProperties settings) throws Exception {
		final Admin admin = Admin.create(Map.of("bootstrap.servers", settings.bootstrapServers()));
		try {
			admin.createTopics(List.of(new NewTopic(settings.topic(), 1, (short) 1)))
				.all().get(10, TimeUnit.SECONDS);
		} finally {
			admin.close(REQUEST_TIMEOUT);
		}
	}

	private void assertCommitted(MessagingProperties settings, long expected) {
		final var kafka = new KafkaClients(settings).rawConsumer();
		try {
			final TopicPartition partition = new TopicPartition(settings.topic(), 0);
			assertEquals(expected, kafka.committed(Set.of(partition), REQUEST_TIMEOUT).get(partition).offset());
		} finally {
			kafka.close(CloseOptions.timeout(REQUEST_TIMEOUT));
		}
	}
}
