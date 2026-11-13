package com.example.grpcavrolab.avro.config;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import java.util.Map;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
class MessagingConfiguration {
	@Bean
	SchemaRegistryClient schemaRegistryClient(MessagingProperties properties) {
		return new CachedSchemaRegistryClient(properties.registryUrl().toString(), properties.registryCacheCapacity(),
			registryOptions(properties));
	}

	static Map<String, Object> registryOptions(MessagingProperties properties) {
		final int timeoutMs = Math.toIntExact(properties.requestTimeout().toMillis());
		return Map.of(
			SchemaRegistryClientConfig.HTTP_CONNECT_TIMEOUT_MS, timeoutMs,
			SchemaRegistryClientConfig.HTTP_READ_TIMEOUT_MS, timeoutMs,
			SchemaRegistryClientConfig.MAX_RETRIES_CONFIG, 0
		);
	}
}
