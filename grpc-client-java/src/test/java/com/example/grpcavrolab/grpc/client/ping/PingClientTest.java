package com.example.grpcavrolab.grpc.client.ping;

import com.example.grpcavrolab.contract.ping.v1.PingRequest;
import com.example.grpcavrolab.contract.ping.v1.PingResponse;
import com.example.grpcavrolab.contract.ping.v1.PingServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

class PingClientTest {
	@Test
	void reusesChannelForSuccessfulRequests() throws Exception {
		withServer(new PingServiceGrpc.PingServiceImplBase() {
			@Override
			public void ping(PingRequest request, StreamObserver<PingResponse> observer) {
				assertEquals("Ping", request.getMessage());
				observer.onNext(PingResponse.newBuilder().setMessage("Pong").build());
				observer.onCompleted();
			}
		}, client -> {
			assertEquals("Pong", client.ping("Ping"));
			assertEquals("Pong", client.ping("Ping"));
		});
	}

	@Test
	void propagatesServerStatus() throws Exception {
		withServer(new PingServiceGrpc.PingServiceImplBase() {
			@Override
			public void ping(PingRequest request, StreamObserver<PingResponse> observer) {
				observer.onError(Status.INVALID_ARGUMENT.withDescription("invalid").asRuntimeException());
			}
		}, client -> assertEquals(Status.Code.INVALID_ARGUMENT,
			assertThrows(StatusRuntimeException.class, () -> client.ping("bad")).getStatus().getCode()));
	}

	@Test
	void deadlineTerminatesUnresponsiveServiceWithoutSleeps() throws Exception {
		withServer(new PingServiceGrpc.PingServiceImplBase() {
			@Override
			public void ping(PingRequest request, StreamObserver<PingResponse> observer) {
				// Leave the call open: the client's deadline must terminate it.
			}
		}, client -> assertTimeoutPreemptively(Duration.ofSeconds(3), () ->
			assertEquals(Status.Code.DEADLINE_EXCEEDED,
				assertThrows(StatusRuntimeException.class, () -> client.ping("Ping")).getStatus().getCode())));
	}

	private void withServer(PingServiceGrpc.PingServiceImplBase service,
		java.util.function.Consumer<PingClient> verification) throws Exception {
		final String name = InProcessServerBuilder.generateName();
		final Server server = InProcessServerBuilder.forName(name).directExecutor().addService(service).build().start();
		final ManagedChannel channel = InProcessChannelBuilder.forName(name).directExecutor().build();
		try {
			verification.accept(new PingClient(PingServiceGrpc.newBlockingStub(channel), Duration.ofMillis(500)));
		} finally {
			channel.shutdownNow().awaitTermination(2, TimeUnit.SECONDS);
			server.shutdownNow().awaitTermination(2, TimeUnit.SECONDS);
		}
	}
}
