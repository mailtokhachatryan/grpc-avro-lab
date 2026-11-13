package com.example.grpcavrolab.avro;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication(proxyBeanMethods = false)
@ConfigurationPropertiesScan
public class AvroMessagingApplication {
	public static void main(String[] args) {
		System.exit(MessagingLauncher.run(args, System.out, System.err));
	}
}
