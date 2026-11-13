package com.example.grpcavrolab.avro;

import com.example.grpcavrolab.avro.event.MessagingCommands;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.common.KafkaException;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;

final class MessagingLauncher {
	private static final String HELP = """
		Usage: avro-messaging <produce|consume|inspect> [options]
		  produce --schema v1|v2 --message TEXT [--event-id ID] [--source TEXT]
		  consume [--reader-schema writer|v1|v2|incompatible] [--count N] [--group ID]
		  inspect [--count N]
		Common options: --topic NAME --bootstrap-servers HOST:PORT --registry-url URL --timeout DURATION
		""";

	private MessagingLauncher() {}

	static int run(String[] args, PrintStream output, PrintStream error) {
		if (args.length == 0 || "--help".equals(args[0]) || (args.length == 2 && "--help".equals(args[1]))) {
			output.print(HELP);
			return 0;
		}
		try {
			final MessagingOptions options = MessagingOptions.parse(args);
			final ConfigurableApplicationContext context;
			try {
				context = SpringApplication.run(AvroMessagingApplication.class, options.springArguments());
			} catch (RuntimeException exception) {
				// Spring reports configuration and startup errors through its logger.
				return 2;
			}
			try (context) {
				context.getBean(MessagingCommands.class).execute(options, output);
				return 0;
			}
		} catch (IllegalArgumentException exception) {
			error.println("Invalid command: " + exception.getMessage());
			return 2;
		} catch (InterruptedException exception) {
			Thread.currentThread().interrupt();
			error.println("Messaging command interrupted");
			return 1;
		} catch (TimeoutException exception) {
			error.println("Kafka acknowledgement timed out; delivery may have occurred");
			return 1;
		} catch (IOException | RestClientException | ExecutionException | KafkaException exception) {
			error.println("Messaging command failed: " + exception.getMessage());
			return 1;
		}
	}
}
