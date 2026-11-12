package com.example.grpcavrolab.grpc.client.ping;

import io.grpc.StatusRuntimeException;
import java.io.PrintStream;

/** Executes one RPC and keeps its response separate from diagnostics. */
public final class PingClientCommand {
	private final PingClient client;
	private final String message;

	PingClientCommand(PingClient client, String message) {
		this.client = client;
		this.message = message;
	}

	/** @return zero on success or one on RPC failure */
	public int execute(PrintStream output, PrintStream error) {
		try {
			output.println(client.ping(message));
			return 0;
		} catch (StatusRuntimeException exception) {
			error.printf("RPC failed: %s%n", exception.getStatus().getCode());
			return 1;
		}
	}
}
