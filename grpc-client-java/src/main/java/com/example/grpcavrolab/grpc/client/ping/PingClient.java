package com.example.grpcavrolab.grpc.client.ping;

import com.example.grpcavrolab.contract.ping.v1.PingRequest;
import com.example.grpcavrolab.contract.ping.v1.PingServiceGrpc;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/** Calls Ping with an independent deadline on each RPC. The context owns the channel. */
public final class PingClient {
	private final PingServiceGrpc.PingServiceBlockingStub stub;
	private final Duration deadline;

	PingClient(PingServiceGrpc.PingServiceBlockingStub stub, Duration deadline) {
		this.stub = Objects.requireNonNull(stub);
		this.deadline = Objects.requireNonNull(deadline);
	}

	/** @return the response message; gRPC status failures propagate to the caller */
	public String ping(String message) {
		return stub.withDeadlineAfter(deadline.toNanos(), TimeUnit.NANOSECONDS)
			.ping(PingRequest.newBuilder().setMessage(message).build()).getMessage();
	}
}
