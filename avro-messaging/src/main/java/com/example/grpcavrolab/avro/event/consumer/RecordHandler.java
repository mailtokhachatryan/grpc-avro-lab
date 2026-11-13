package com.example.grpcavrolab.avro.event.consumer;

import java.io.IOException;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/** Processes one record; an exception must leave its offset uncommitted. */
@FunctionalInterface
public interface RecordHandler<V> {
	void process(ConsumerRecord<String, V> record) throws IOException;
}
