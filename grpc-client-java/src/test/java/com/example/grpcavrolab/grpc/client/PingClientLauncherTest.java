package com.example.grpcavrolab.grpc.client;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PingClientLauncherTest {
	@Test
	void helpDoesNotRequireAConnection() {
		final ByteArrayOutputStream output = new ByteArrayOutputStream();
		final ByteArrayOutputStream error = new ByteArrayOutputStream();
		assertEquals(0, PingClientLauncher.run(new String[]{"--help"}, stream(output), stream(error)));
		assertTrue(output.toString(StandardCharsets.UTF_8).contains("--deadline-ms"));
		assertEquals(0, error.size());
	}

	@Test
	void missingOptionValueFailsWithoutPrintingAResponse() {
		final ByteArrayOutputStream output = new ByteArrayOutputStream();
		final ByteArrayOutputStream error = new ByteArrayOutputStream();
		assertEquals(2, PingClientLauncher.run(new String[]{"--port", "--message", "Ping"}, stream(output), stream(error)));
		assertTrue(error.toString(StandardCharsets.UTF_8).contains("Missing value for --port"));
		assertEquals(0, output.size());
	}

	private PrintStream stream(ByteArrayOutputStream bytes) {
		return new PrintStream(bytes, true, StandardCharsets.UTF_8);
	}
}
