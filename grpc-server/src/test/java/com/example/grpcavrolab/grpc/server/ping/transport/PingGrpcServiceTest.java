package com.example.grpcavrolab.grpc.server.ping.transport;

import com.example.grpcavrolab.contract.ping.v1.PingRequest;
import com.example.grpcavrolab.contract.ping.v1.PingServiceGrpc;
import com.example.grpcavrolab.grpc.server.ping.service.PingService;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PingGrpcServiceTest {
	private Server server;
	private ManagedChannel channel;
	private PingServiceGrpc.PingServiceBlockingStub stub;

	@BeforeEach
	void start() throws Exception {
		final String name = InProcessServerBuilder.generateName();
		server = InProcessServerBuilder.forName(name).directExecutor()
			.addService(new PingGrpcService(new PingService())).build().start();
		channel = InProcessChannelBuilder.forName(name).directExecutor().build();
		stub = PingServiceGrpc.newBlockingStub(channel).withDeadlineAfter(2, TimeUnit.SECONDS);
	}

	@AfterEach
	void stop() throws Exception {
		channel.shutdownNow().awaitTermination(2, TimeUnit.SECONDS);
		server.shutdownNow().awaitTermination(2, TimeUnit.SECONDS);
	}

	@Test
	void returnsPongThroughGeneratedStub() {
		assertEquals("Pong", stub.ping(PingRequest.newBuilder().setMessage("Ping").build()).getMessage());
	}

	@ParameterizedTest
	@ValueSource(strings = {"", " ", "ping", "Pong", " Ping "})
	void rejectsInvalidMessagesWithSafeStatus(String message) {
		final StatusRuntimeException failure = assertThrows(StatusRuntimeException.class,
			() -> stub.ping(PingRequest.newBuilder().setMessage(message).build()));
		assertEquals(Status.Code.INVALID_ARGUMENT, failure.getStatus().getCode());
		assertEquals("message must be exactly Ping", failure.getStatus().getDescription());
	}

	@Test
	void rejectsMissingMessage() {
		assertEquals(Status.Code.INVALID_ARGUMENT, assertThrows(StatusRuntimeException.class,
			() -> stub.ping(PingRequest.getDefaultInstance())).getStatus().getCode());
	}
}
