package com.mgoode;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.task.configuration.EnableTask;
import org.springframework.context.annotation.ComponentScan;



@EnableTask
@EnableBatchProcessing
@SpringBootApplication
public class SpringBatchEdiloaderDemoApplication {
	public static void main(String[] args) {
		SpringApplication.run(SpringBatchEdiloaderDemoApplication.class, args);
	}
}
