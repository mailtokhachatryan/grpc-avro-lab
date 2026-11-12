package com.example.grpcavrolab.grpc.client.ping;

import com.example.grpcavrolab.contract.ping.v1.PingServiceGrpc;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.grpc.client.ImportGrpcClients;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(PingClientProperties.class)
@ImportGrpcClients(target = "ping", types = PingServiceGrpc.PingServiceBlockingStub.class)
class PingClientConfiguration {
	@Bean
	PingClient pingClient(PingServiceGrpc.PingServiceBlockingStub stub, PingClientProperties properties) {
		return new PingClient(stub, properties.deadline());
	}

	@Bean
	PingClientCommand pingClientCommand(PingClient client, PingClientProperties properties) {
		return new PingClientCommand(client, properties.message());
	}
}
