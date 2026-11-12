package com.example.grpcavrolab.grpc.server.ping.transport;

import java.net.InetAddress;
import org.springframework.boot.grpc.server.autoconfigure.GrpcServerFactoryCustomizer;
import org.springframework.boot.grpc.server.autoconfigure.GrpcServerProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Guards the lab's network boundary before the managed server starts. */
@Configuration(proxyBeanMethods = false)
class PingServerConfiguration {
	@Bean
	GrpcServerFactoryCustomizer validateServerConfiguration(GrpcServerProperties properties) {
		return factory -> {
			final InetAddress address = properties.getAddress();
			if (address == null || !address.isLoopbackAddress()) {
				throw new IllegalArgumentException("spring.grpc.server.address must be a loopback address");
			}
			if (properties.getShutdown().getGracePeriod().isNegative()) {
				throw new IllegalArgumentException("spring.grpc.server.shutdown.grace-period must not be negative");
			}
		};
	}
}
