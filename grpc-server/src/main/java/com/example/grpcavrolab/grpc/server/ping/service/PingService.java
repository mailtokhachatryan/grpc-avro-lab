package com.example.grpcavrolab.grpc.server.ping.service;

import com.example.grpcavrolab.contract.ping.v1.PingMessages;
import org.springframework.stereotype.Service;

/** Owns the exact, case-sensitive Ping/Pong rule independently of transport. */
@Service
public final class PingService {
	/**
	 * @param message the exact string {@code Ping}
	 * @return {@code Pong}
	 * @throws InvalidPingMessageException when the message is not exactly Ping
	 */
	public String ping(String message) {
		if (!PingMessages.REQUEST.equals(message)) {
			throw new InvalidPingMessageException();
		}
		return PingMessages.RESPONSE;
	}
}
