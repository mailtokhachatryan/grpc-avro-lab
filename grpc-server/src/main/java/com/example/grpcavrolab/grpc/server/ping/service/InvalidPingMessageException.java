package com.example.grpcavrolab.grpc.server.ping.service;

import com.example.grpcavrolab.contract.ping.v1.PingMessages;

/** Indicates that the request did not contain the protocol's expected message. */
public final class InvalidPingMessageException extends IllegalArgumentException {
	public InvalidPingMessageException() {
		super("message must be exactly " + PingMessages.REQUEST);
	}
}
