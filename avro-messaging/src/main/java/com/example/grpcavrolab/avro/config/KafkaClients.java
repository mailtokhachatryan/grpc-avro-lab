package com.example.grpcavrolab.avro.config;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Component;

/** Creates a client only for the selected command; the command owns its lifecycle. */
@Component
public final class KafkaClients {
	private final MessagingProperties properties;

	public KafkaClients(MessagingProperties properties) {
		this.properties = properties;
	}

	public KafkaProducer<String, GenericRecord> producer() {
		final Map<String, Object> config = commonConfig();
		config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
		config.put(ProducerConfig.ACKS_CONFIG, "all");
		config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
		config.put(ProducerConfig.LINGER_MS_CONFIG, 0);
		config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, properties.requestTimeout().toMillis());
		config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,
			Math.toIntExact(Math.max(properties.timeout().toMillis(), properties.requestTimeout().toMillis())));
		config.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, true);
		config.put(AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION, false);
		config.put(AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicNameStrategy.class);
		return new KafkaProducer<>(config);
	}

	public KafkaConsumer<String, GenericRecord> avroConsumer() {
		final Map<String, Object> config = consumerConfig();
		config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
		config.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);
		return new KafkaConsumer<>(config);
	}

	/** Raw reader uses explicit partition assignment and no consumer group. */
	public KafkaConsumer<String, byte[]> inspector() {
		final Map<String, Object> config = consumerConfig();
		config.remove(ConsumerConfig.GROUP_ID_CONFIG);
		config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
		return new KafkaConsumer<>(config);
	}

	public KafkaConsumer<String, byte[]> rawConsumer() {
		final Map<String, Object> config = consumerConfig();
		config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
		return new KafkaConsumer<>(config);
	}

	private Map<String, Object> consumerConfig() {
		final Map<String, Object> config = commonConfig();
		config.put(ConsumerConfig.GROUP_ID_CONFIG, properties.group());
		config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		config.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
		return config;
	}

	private Map<String, Object> commonConfig() {
		final Map<String, Object> config = new HashMap<>(MessagingConfiguration.registryOptions(properties));
		config.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, properties.bootstrapServers());
		config.put(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, Math.toIntExact(properties.requestTimeout().toMillis()));
		config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, properties.registryUrl().toString());
		return config;
	}
}
