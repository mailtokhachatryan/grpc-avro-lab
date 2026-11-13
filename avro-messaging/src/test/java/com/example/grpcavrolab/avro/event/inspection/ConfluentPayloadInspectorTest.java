package com.example.grpcavrolab.avro.event.inspection;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ConfluentPayloadInspectorTest {
	@Test
	void extractsOnlyTheFiveByteBigEndianPrefix() {
		final byte[] value = {0, 0, 1, 2, 3, 4, 5};
		assertEquals(0x00010203, ConfluentPayloadInspector.schemaId(value));
		assertEquals("00 00 01 02 03", ConfluentPayloadInspector.prefixHex(value));
	}

	@Test
	void rejectsNullShortUnknownAndInvalidPrefixes() {
		assertThrows(IllegalArgumentException.class, () -> ConfluentPayloadInspector.schemaId(null));
		assertThrows(IllegalArgumentException.class, () -> ConfluentPayloadInspector.schemaId(new byte[4]));
		assertThrows(IllegalArgumentException.class,
			() -> ConfluentPayloadInspector.schemaId(new byte[]{1, 0, 0, 0, 1}));
		assertThrows(IllegalArgumentException.class, () -> ConfluentPayloadInspector.schemaId(new byte[5]));
	}
}
