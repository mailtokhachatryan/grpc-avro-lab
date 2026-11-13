package com.example.grpcavrolab.avro.event.consumer;

import com.example.grpcavrolab.avro.config.MessagingProperties;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MessageConsumerTest {
	private static final String TOPIC = "lab.unit";
	private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);

	@Test
	void commitsOnlySuccessfullyProcessedRecordsBeforeAnOutputFailure() {
		final var properties = new MessagingProperties(List.of("localhost:9092"), URI.create("http://localhost:8081"),
			TOPIC, "unit-group", Duration.ofSeconds(1), Duration.ofMillis(100), Duration.ofMillis(10),
			Duration.ofMillis(100), 16);
		final List<Long> committedOffsets = new ArrayList<>();
		final MockConsumer<String, String> kafka = new MockConsumer<>("earliest") {
			@Override
			public void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets, Duration timeout) {
				committedOffsets.add(offsets.get(PARTITION).offset());
				super.commitSync(offsets, timeout);
			}
		};
		kafka.schedulePollTask(() -> {
			kafka.rebalance(List.of(PARTITION));
			kafka.updateBeginningOffsets(Map.of(PARTITION, 7L));
			kafka.addRecord(new ConsumerRecord<>(TOPIC, 0, 7, "ok", "first"));
			kafka.addRecord(new ConsumerRecord<>(TOPIC, 0, 8, "bad", "second"));
			kafka.addRecord(new ConsumerRecord<>(TOPIC, 0, 9, "later", "third"));
		});

		final IOException failure = assertThrows(IOException.class,
			() -> new MessageConsumer(properties).consume(kafka, 3, record -> {
				if ("bad".equals(record.key())) {
					throw new IOException("stdout failed");
				}
			}));
		assertTrue(failure.getMessage().contains("offset 8"));
		assertEquals(List.of(8L), committedOffsets);
		assertTrue(kafka.closed());
	}
}
