package com.example.grpcavrolab.avro.event.inspection;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HexFormat;

/** Parses the Confluent version-zero value prefix used by the lab. */
public final class ConfluentPayloadInspector {
	public static final int PREFIX_LENGTH = Byte.BYTES + Integer.BYTES;
	private static final byte MAGIC = 0;

	private ConfluentPayloadInspector() {}

	/** @return the big-endian schema ID carried in bytes 1 through 4 */
	public static int schemaId(byte[] value) {
		if (value == null || value.length < PREFIX_LENGTH) {
			throw new IllegalArgumentException("Expected a non-null Avro value with a five-byte prefix");
		}
		if (value[0] != MAGIC) {
			throw new IllegalArgumentException("Unsupported Confluent payload version: " + Byte.toUnsignedInt(value[0]));
		}
		final int id = ByteBuffer.wrap(value, Byte.BYTES, Integer.BYTES).order(ByteOrder.BIG_ENDIAN).getInt();
		if (id < 1) {
			throw new IllegalArgumentException("Expected a positive schema ID");
		}
		return id;
	}

	public static String prefixHex(byte[] value) {
		schemaId(value);
		return HexFormat.of().withDelimiter(" ").formatHex(value, 0, PREFIX_LENGTH);
	}
}
