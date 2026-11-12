package com.example.grpcavrolab.grpc.client;

import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(proxyBeanMethods = false)
public class PingClientApplication {
	public static void main(String[] args) {
		System.exit(PingClientLauncher.run(args, System.out, System.err));
	}
}
