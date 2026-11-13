package com.example.grpcavrolab.avro.event.consumer;

import com.example.grpcavrolab.avro.config.MessagingProperties;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;

@Component
public final class MessageConsumer {
	private final MessagingProperties properties;

	public MessageConsumer(MessagingProperties properties) {
		this.properties = properties;
	}

	/** Closes the owned consumer and commits only records whose handler succeeded. */
	public <V> void consume(Consumer<String, V> consumer, int count, RecordHandler<V> handler) throws IOException {
		read(consumer, count, handler, true);
	}

	/** Replays a topic from its earliest offsets without joining or committing a group. */
	public <V> void inspect(Consumer<String, V> consumer, int count, RecordHandler<V> handler) throws IOException {
		read(consumer, count, handler, false);
	}

	private <V> void read(Consumer<String, V> consumer, int count, RecordHandler<V> handler, boolean commit)
		throws IOException {
		final CountDownLatch closed = new CountDownLatch(1);
		final Thread shutdown = new Thread(() -> {
			consumer.wakeup();
			try {
				closed.await(properties.closeTimeout().plus(properties.requestTimeout()).toMillis(),
					TimeUnit.MILLISECONDS);
			} catch (InterruptedException exception) {
				Thread.currentThread().interrupt();
			}
		}, "kafka-consumer-shutdown");
		Runtime.getRuntime().addShutdownHook(shutdown);
		try {
			if (commit) {
				consumer.subscribe(List.of(properties.topic()));
			} else {
				final var partitions = consumer.partitionsFor(properties.topic(), properties.requestTimeout()).stream()
					.map(partition -> new TopicPartition(partition.topic(), partition.partition())).toList();
				if (partitions.isEmpty()) {
					throw new IOException("No partitions found for " + properties.topic());
				}
				consumer.assign(partitions);
				consumer.seekToBeginning(partitions);
			}
			readUntilCount(consumer, count, handler, commit);
		} catch (WakeupException exception) {
			// The shutdown hook is the only caller of wakeup().
		} catch (RecordDeserializationException exception) {
			throw new IOException("Could not deserialize " + exception.topicPartition()
				+ " offset " + exception.offset(), exception);
		} finally {
			try {
				consumer.close(CloseOptions.timeout(properties.closeTimeout()));
			} finally {
				closed.countDown();
				try {
					Runtime.getRuntime().removeShutdownHook(shutdown);
				} catch (IllegalStateException ignored) {
					// The JVM is already shutting down.
				}
			}
		}
	}

	private <V> void readUntilCount(Consumer<String, V> consumer, int count, RecordHandler<V> handler, boolean commit)
		throws IOException {
		final long deadline = System.nanoTime() + properties.timeout().toNanos();
		int processed = 0;
		while (processed < count) {
			final Duration remaining = Duration.ofNanos(deadline - System.nanoTime());
			if (remaining.isNegative() || remaining.isZero()) {
				throw new IOException("Timed out after reading " + processed + " of " + count + " records");
			}
			final Duration pollTime = remaining.compareTo(properties.pollInterval()) < 0
				? remaining : properties.pollInterval();
			for (final var record : consumer.poll(pollTime)) {
				try {
					handler.process(record);
				} catch (IOException | RuntimeException exception) {
					throw new IOException("Could not process " + record.topic() + " partition " + record.partition()
						+ " offset " + record.offset(), exception);
				}
				if (commit) {
					final var partition = new TopicPartition(record.topic(), record.partition());
					consumer.commitSync(Map.of(partition, new OffsetAndMetadata(record.offset() + 1)),
						properties.requestTimeout());
				}
				if (++processed == count) {
					return;
				}
			}
		}
	}
}
