package com.example.grpcavrolab.avro;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/** Parses the CLI before any Spring beans or external clients are created. */
public final class MessagingOptions {
	private static final Map<String, String> COMMON_OPTIONS = Map.of(
		"topic", "lab.messaging.topic",
		"bootstrap-servers", "lab.messaging.bootstrap-servers",
		"registry-url", "lab.messaging.registry-url",
		"timeout", "lab.messaging.timeout"
	);
	private static final Map<String, Set<String>> COMMAND_OPTIONS = Map.of(
		"produce", Set.of("schema", "message", "event-id", "source"),
		"consume", Set.of("reader-schema", "count", "group"),
		"inspect", Set.of("count")
	);
	private static final int MAX_COUNT = 10_000;
	private static final int MAX_MESSAGE_LENGTH = 4096;

	private final String command;
	private final Map<String, String> values;
	private final String[] springArguments;

	private MessagingOptions(String command, Map<String, String> values, List<String> springArguments) {
		this.command = command;
		this.values = Map.copyOf(values);
		this.springArguments = springArguments.toArray(String[]::new);
	}

	static MessagingOptions parse(String[] args) {
		final String command = args[0];
		final Set<String> allowed = COMMAND_OPTIONS.get(command);
		if (allowed == null) {
			throw new IllegalArgumentException("Expected produce, consume, or inspect");
		}
		final Map<String, String> values = new HashMap<>();
		final List<String> springArguments = new ArrayList<>();
		for (int index = 1; index < args.length; index++) {
			final String argument = args[index];
			if (!argument.startsWith("--")) {
				throw new IllegalArgumentException("Expected a named option");
			}
			final int equals = argument.indexOf('=');
			final String name = argument.substring(2, equals < 0 ? argument.length() : equals);
			if (!allowed.contains(name) && !COMMON_OPTIONS.containsKey(name)) {
				throw new IllegalArgumentException("Unknown option --" + name + " for " + command);
			}
			if (equals < 0 && (index + 1 == args.length || args[index + 1].startsWith("--"))) {
				throw new IllegalArgumentException("Missing value for --" + name);
			}
			final String value = equals < 0 ? args[++index] : argument.substring(equals + 1);
			if (values.putIfAbsent(name, value) != null) {
				throw new IllegalArgumentException("Repeated option --" + name);
			}
			final String property = "group".equals(name) ? "lab.messaging.group" : COMMON_OPTIONS.get(name);
			if (property != null) {
				springArguments.add("--" + property + "=" + value);
			}
		}
		return new MessagingOptions(command, values, springArguments);
	}

	public String command() {
		return command;
	}

	public String writerVersion() {
		return values.getOrDefault("schema", "v1");
	}

	public String readerVersion() {
		return values.getOrDefault("reader-schema", "writer");
	}

	public String message() {
		final String message = values.get("message");
		if (message == null || message.isBlank() || message.length() > MAX_MESSAGE_LENGTH) {
			throw new IllegalArgumentException("--message must contain 1–" + MAX_MESSAGE_LENGTH + " characters");
		}
		return message;
	}

	public String eventId() {
		final String eventId = values.get("event-id");
		if (eventId == null) {
			return UUID.randomUUID().toString();
		}
		if (eventId.isBlank() || eventId.length() > MAX_MESSAGE_LENGTH) {
			throw new IllegalArgumentException("--event-id must contain 1–" + MAX_MESSAGE_LENGTH + " characters");
		}
		return eventId;
	}

	public String source() {
		final String source = values.get("source");
		if (source != null && source.length() > MAX_MESSAGE_LENGTH) {
			throw new IllegalArgumentException("--source must not exceed " + MAX_MESSAGE_LENGTH + " characters");
		}
		return source;
	}

	public int count() {
		try {
			final int count = Integer.parseInt(values.getOrDefault("count", "1"));
			if (count > 0 && count <= MAX_COUNT) {
				return count;
			}
		} catch (NumberFormatException ignored) {
			// Report the same range error for malformed and out-of-range values.
		}
		throw new IllegalArgumentException("--count must be an integer between 1 and " + MAX_COUNT);
	}

	String[] springArguments() {
		return Arrays.copyOf(springArguments, springArguments.length);
	}
}
