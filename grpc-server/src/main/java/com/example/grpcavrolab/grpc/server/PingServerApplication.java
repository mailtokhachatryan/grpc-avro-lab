package com.example.grpcavrolab.grpc.server;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(proxyBeanMethods = false)
public class PingServerApplication {
	public static void main(String[] args) {
		SpringApplication.run(PingServerApplication.class, args);
	}
}
