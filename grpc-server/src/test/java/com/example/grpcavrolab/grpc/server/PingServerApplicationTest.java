package com.example.grpcavrolab.grpc.server;

import com.example.grpcavrolab.contract.ping.v1.PingRequest;
import com.example.grpcavrolab.contract.ping.v1.PingServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationListener;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.grpc.server.lifecycle.GrpcServerStartedEvent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(20)
class PingServerApplicationTest {
	@Test
	void bootStartsPingAndHealthAndClosesTheServerWithTheContext() throws Exception {
		final AtomicReference<GrpcServerStartedEvent> started = new AtomicReference<>();
		final SpringApplication application = application();
		application.addListeners((ApplicationListener<GrpcServerStartedEvent>) started::set);

		try (var context = application.run("--spring.grpc.server.port=0",
			"--spring.grpc.server.address=127.0.0.1", "--spring.grpc.server.reflection.enabled=false")) {
			final GrpcServerStartedEvent event = started.get();
			assertNotNull(event);
			final ManagedChannel channel = ManagedChannelBuilder.forAddress("127.0.0.1", event.getPort())
				.usePlaintext().build();
			try {
				assertEquals("Pong", PingServiceGrpc.newBlockingStub(channel).withDeadlineAfter(3, TimeUnit.SECONDS)
					.ping(PingRequest.newBuilder().setMessage("Ping").build()).getMessage());
				final HealthCheckRequest request = HealthCheckRequest.newBuilder()
					.setService(PingServiceGrpc.SERVICE_NAME).build();
				final Iterator<HealthCheckResponse> statuses = HealthGrpc.newBlockingStub(channel)
					.withDeadlineAfter(5, TimeUnit.SECONDS).watch(request);
				HealthCheckResponse status;
				do {
					status = statuses.next();
				} while (status.getStatus() != HealthCheckResponse.ServingStatus.SERVING);
				assertEquals(HealthCheckResponse.ServingStatus.SERVING, HealthGrpc.newBlockingStub(channel)
					.withDeadlineAfter(3, TimeUnit.SECONDS).check(request).getStatus());
			} finally {
				channel.shutdownNow().awaitTermination(3, TimeUnit.SECONDS);
			}
		}
		assertTrue(started.get().getServer().isShutdown());
	}

	@ParameterizedTest
	@CsvSource({
		"spring.grpc.server.address=0.0.0.0, loopback address",
		"spring.grpc.server.shutdown.grace-period=-1s, must not be negative"
	})
	void rejectsUnsafeSettingsBeforeBinding(String property, String expectedError) {
		final RuntimeException failure = assertThrows(RuntimeException.class, () -> {
			try (var context = application().run("--spring.grpc.server.port=0", "--" + property)) {
				// Close the context if invalid settings are unexpectedly accepted.
			}
		});
		assertTrue(NestedExceptionUtils.getMostSpecificCause(failure).getMessage().contains(expectedError));
	}

	private SpringApplication application() {
		final SpringApplication application = new SpringApplication(PingServerApplication.class);
		application.setRegisterShutdownHook(false);
		return application;
	}
}
