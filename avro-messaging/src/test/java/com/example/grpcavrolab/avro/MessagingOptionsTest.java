package com.example.grpcavrolab.avro;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MessagingOptionsTest {
	@Test
	void convertsRuntimeOverridesToSpringProperties() {
		final MessagingOptions options = MessagingOptions.parse(new String[]{
			"consume", "--group=demo", "--topic", "lab.other", "--count", "2", "--reader-schema", "v2"
		});

		assertArrayEquals(new String[]{"--lab.messaging.group=demo", "--lab.messaging.topic=lab.other"},
			options.springArguments());
		assertEquals(2, options.count());
		assertEquals("v2", options.readerVersion());
	}

	@Test
	void rejectsAmbiguousAndCommandSpecificOptions() {
		assertThrows(IllegalArgumentException.class,
			() -> MessagingOptions.parse(new String[]{"produce", "--message=a", "--message=b"}));
		assertThrows(IllegalArgumentException.class,
			() -> MessagingOptions.parse(new String[]{"inspect", "--group", "real-consumers"}));
		assertThrows(IllegalArgumentException.class,
			() -> MessagingOptions.parse(new String[]{"consume", "--count", "0"}).count());
		assertThrows(IllegalArgumentException.class,
			() -> MessagingOptions.parse(new String[]{"produce", "--message", "--topic", "lab.messages"}));
	}
}
