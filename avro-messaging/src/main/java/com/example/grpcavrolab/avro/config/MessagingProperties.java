package com.example.grpcavrolab.avro.config;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.regex.Pattern;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** Validated settings shared by the one-shot messaging commands. */
@ConfigurationProperties("lab.messaging")
public record MessagingProperties(
	List<String> bootstrapServers,
	URI registryUrl,
	String topic,
	String group,
	Duration timeout,
	Duration requestTimeout,
	Duration pollInterval,
	Duration closeTimeout,
	int registryCacheCapacity
) {
	private static final Pattern TOPIC_NAME = Pattern.compile("[a-zA-Z0-9._-]{1,249}");
	private static final Duration MAX_TIMEOUT = Duration.ofMinutes(5);

	public MessagingProperties {
		if (bootstrapServers == null || bootstrapServers.isEmpty() || bootstrapServers.stream().anyMatch(String::isBlank)) {
			throw new IllegalArgumentException("bootstrap-servers must contain at least one address");
		}
		bootstrapServers = List.copyOf(bootstrapServers);
		if (registryUrl == null || registryUrl.getHost() == null || registryUrl.getUserInfo() != null
			|| !List.of("http", "https").contains(registryUrl.getScheme())) {
			throw new IllegalArgumentException("registry-url must be an HTTP(S) URL without embedded credentials");
		}
		if (topic == null || !TOPIC_NAME.matcher(topic).matches() || ".".equals(topic) || "..".equals(topic)) {
			throw new IllegalArgumentException("topic is not a valid Kafka topic name");
		}
		if (group == null || group.isBlank()) {
			throw new IllegalArgumentException("group must not be blank");
		}
		validateDuration("timeout", timeout);
		validateDuration("request-timeout", requestTimeout);
		validateDuration("poll-interval", pollInterval);
		validateDuration("close-timeout", closeTimeout);
		if (registryCacheCapacity < 1) {
			throw new IllegalArgumentException("registry-cache-capacity must be positive");
		}
	}

	public String subject() {
		return topic + "-value";
	}

	private static void validateDuration(String name, Duration duration) {
		if (duration == null || duration.compareTo(Duration.ofMillis(1)) < 0 || duration.compareTo(MAX_TIMEOUT) > 0) {
			throw new IllegalArgumentException(name + " must be between 1ms and " + MAX_TIMEOUT);
		}
	}
}
