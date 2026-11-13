package com.example.grpcavrolab.avro.event.schema;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.springframework.stereotype.Component;

/** Loads the canonical Avro contracts shipped as application resources. */
@Component
public final class MessageSchemas {
	public static final String EVENT_ID = "event_id";
	public static final String MESSAGE = "message";
	public static final String SOURCE = "source";

	public Schema writer(String version) {
		return switch (version) {
			case "v1", "v2" -> load(version);
			default -> throw new IllegalArgumentException("Writer schema must be v1 or v2");
		};
	}

	public Schema reader(String version) {
		return switch (version) {
			case "v1", "v2", "incompatible" -> load(version);
			default -> throw new IllegalArgumentException("Reader schema must be v1, v2, or incompatible");
		};
	}

	public GenericRecord event(Schema schema, String eventId, String message, String source) {
		final GenericRecordBuilder builder = new GenericRecordBuilder(schema)
			.set(EVENT_ID, eventId)
			.set(MESSAGE, message);
		if (schema.getField(SOURCE) != null) {
			builder.set(SOURCE, source);
		} else if (source != null) {
			throw new IllegalArgumentException("--source requires writer schema v2");
		}
		return builder.build();
	}

	private Schema load(String version) {
		final String resource = "/avro/" + version + "/message-event.avsc";
		try (InputStream input = MessageSchemas.class.getResourceAsStream(resource)) {
			if (input == null) {
				throw new IllegalStateException("Missing schema resource " + resource);
			}
			return new Schema.Parser().parse(input);
		} catch (IOException exception) {
			throw new UncheckedIOException("Could not read " + resource, exception);
		}
	}
}
