package com.example.grpcavrolab.grpc.client;

import com.example.grpcavrolab.grpc.client.ping.PingClientCommand;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;

final class PingClientLauncher {
	private static final String HELP = "--help";
	private static final String DEADLINE_OPTION = "--deadline-ms";
	private static final int INVALID_CONFIGURATION = 2;
	private static final Map<String, String> OPTIONS = Map.of(
		"--host", "ping.client.host",
		"--port", "ping.client.port",
		DEADLINE_OPTION, "ping.client.deadline",
		"--message", "ping.client.message"
	);

	private PingClientLauncher() {}

	static int run(String[] args, PrintStream output, PrintStream error) {
		if (args.length == 1 && HELP.equals(args[0])) {
			output.println("Usage: --host HOST --port PORT --deadline-ms MS --message MESSAGE");
			return 0;
		}

		final String[] springArgs;
		try {
			springArgs = toSpringArguments(args);
		} catch (IllegalArgumentException exception) {
			error.println("Configuration failed: " + exception.getMessage());
			return INVALID_CONFIGURATION;
		}

		final ConfigurableApplicationContext context;
		try {
			context = SpringApplication.run(PingClientApplication.class, springArgs);
		} catch (RuntimeException exception) {
			// Spring has already reported the startup failure through its logging system.
			return INVALID_CONFIGURATION;
		}
		try (context) {
			return context.getBean(PingClientCommand.class).execute(output, error);
		}
	}

	private static String[] toSpringArguments(String[] args) {
		final List<String> springArgs = new ArrayList<>();
		for (int index = 0; index < args.length; index++) {
			final String argument = args[index];
			final int separator = argument.indexOf('=');
			final String flag = separator < 0 ? argument : argument.substring(0, separator);
			final String property = OPTIONS.get(flag);
			if (property == null) {
				throw new IllegalArgumentException("Unknown option " + flag);
			}
			final String value;
			if (separator >= 0) {
				value = argument.substring(separator + 1);
			} else {
				if (index + 1 == args.length || args[index + 1].startsWith("--")) {
					throw new IllegalArgumentException("Missing value for " + flag);
				}
				value = args[++index];
			}
			final String suffix = DEADLINE_OPTION.equals(flag) ? "ms" : "";
			springArgs.add("--" + property + "=" + value + suffix);
		}
		return springArgs.toArray(String[]::new);
	}
}
