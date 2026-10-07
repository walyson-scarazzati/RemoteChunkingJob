package com.springbatch.remotechunkingjob;

import org.springframework.boot.ConfigurableBootstrapContext;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
public class RemoteChunkingJobApplication {

	public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(RemoteChunkingJobApplication.class, args);
        if (args[1].contains("manager")) context.close();
	}

}
