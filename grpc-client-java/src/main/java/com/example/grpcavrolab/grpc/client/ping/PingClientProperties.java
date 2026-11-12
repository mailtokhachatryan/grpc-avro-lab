package com.example.grpcavrolab.grpc.client.ping;

import com.example.grpcavrolab.contract.ping.v1.PingMessages;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/** Validated client settings; the request defaults to the protocol's Ping token. */
@ConfigurationProperties("ping.client")
record PingClientProperties(
	String host,
	int port,
	Duration deadline,
	@DefaultValue(PingMessages.REQUEST) String message
) {
	private static final int MAX_PORT = 65_535;
	private static final Duration MIN_DEADLINE = Duration.ofMillis(1);
	private static final Duration MAX_DEADLINE = Duration.ofMinutes(1);

	PingClientProperties {
		if (host == null || host.isBlank()) {
			throw new IllegalArgumentException("ping.client.host must not be blank");
		}
		if (port < 1 || port > MAX_PORT) {
			throw new IllegalArgumentException("ping.client.port must be between 1 and " + MAX_PORT);
		}
		if (deadline == null || deadline.compareTo(MIN_DEADLINE) < 0 || deadline.compareTo(MAX_DEADLINE) > 0) {
			throw new IllegalArgumentException("ping.client.deadline must be between 1ms and 60s");
		}
	}
}
