package com.example.grpcavrolab.grpc.server.ping.transport;

import com.example.grpcavrolab.contract.ping.v1.PingRequest;
import com.example.grpcavrolab.contract.ping.v1.PingResponse;
import com.example.grpcavrolab.contract.ping.v1.PingServiceGrpc;
import com.example.grpcavrolab.grpc.server.ping.service.InvalidPingMessageException;
import com.example.grpcavrolab.grpc.server.ping.service.PingService;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/** Translates the Ping use case into a unary response or a safe gRPC status. */
@Service
public final class PingGrpcService extends PingServiceGrpc.PingServiceImplBase {
	private static final Logger LOGGER = LoggerFactory.getLogger(PingGrpcService.class);
	private static final Status PROCESSING_FAILURE = Status.INTERNAL.withDescription("Ping processing failed");

	private final PingService service;

	public PingGrpcService(PingService service) {
		this.service = Objects.requireNonNull(service);
	}

	@Override
	public void ping(PingRequest request, StreamObserver<PingResponse> observer) {
		final String response;
		try {
			response = service.ping(request.getMessage());
		} catch (InvalidPingMessageException exception) {
			observer.onError(Status.INVALID_ARGUMENT.withDescription(exception.getMessage()).asRuntimeException());
			return;
		} catch (RuntimeException exception) {
			LOGGER.error("Unexpected failure in {}", PingServiceGrpc.getPingMethod().getFullMethodName(), exception);
			observer.onError(PROCESSING_FAILURE.asRuntimeException());
			return;
		}
		observer.onNext(PingResponse.newBuilder().setMessage(response).build());
		observer.onCompleted();
	}
}
